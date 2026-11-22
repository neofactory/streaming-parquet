#define DUCKDB_EXTENSION_MAIN

// parquet_checkpoint: read live streaming-parquet checkpoints as ordinary
// parquet. Registers a filesystem that presents a checkpoint state
// (chunk objects + footer, per pqc.h) as one read-only virtual file,
// so stock read_parquet() works on a file that is still being
// written.
//
// Dispatch (chosen to make re-entry into ourselves impossible):
//   /path/to/x.parquet.000017.ckpt local path, no scheme: ours
//   pqc://s3://bucket/x.parquet.000017.ckpt
//                                  any scheme, explicitly wrapped:
//                                  ours; the wrapped path is opened
//                                  through DuckDB's own filesystems
//                                  (httpfs etc.), which we never
//                                  shadow because a scheme'd path
//                                  without pqc:// is never claimed.
//
// Integrity: reads are ranged (a column read fetches only its byte
// ranges), so the manifest chunk CRCs are NOT verified here — parquet
// page checksums are the guard on this path, same contract as the
// Python CheckpointFile ranged mode.
//
// Nothing is read twice. Two faults were found on 2026-11-09, and how
// the objects are read answers both:
//
//   The cache. DuckDB keeps what a query read from a remote file and
//   serves the next query from memory, but only for paths it knows as
//   remote (s3://, http://, ...). The path of the virtual file begins
//   with pqc://, so DuckDB caches nothing of it: every query downloaded
//   the checkpoint object again, once for each time DuckDB opened the
//   file, three times in a query.
//
//   The threads. DuckDB 2.0 reads one file handle from every scan
//   thread at once. A handle of ours that opened chunks on first touch
//   and kept them in a table with no lock died or hung.
//
// A chunk is read through DuckDB's own caching file system
// (CachingFileSystem). The chunk has a remote path, so it is cached
// under that path and fetched once, however many checkpoint versions
// name it. It never changes once written, so it is opened with no
// validity check: a cached chunk costs no request at all, not even to
// open it. The caching file system is made for many threads; what is
// left to us is one lock for each chunk, held only while it is opened.
//
// A checkpoint version is kept by us, the last few read
// (KEPT_CHECKPOINTS). A version is opened several times by one query
// and by the queries that follow it, and then never again. DuckDB's
// cache would keep every version it ever read until memory ran short:
// 4 MB each 15 seconds on the stream measured that day.

#include "parquet_checkpoint_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/query_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"

extern "C" {
#include "pqc.h"
}

namespace duckdb {

static constexpr const char *PQC_SCHEME = "pqc://";
static constexpr const char *CKPT_SUFFIX = ".ckpt";

struct CkptSegment {
	idx_t start;      // offset in the virtual file
	idx_t length;
	string path;      // chunk object path; empty = serve from ckpt blob
	idx_t blob_off;   // when path is empty: offset within the ckpt blob
};

// One checkpoint object as it was read: its bytes and its time in the
// store. Never changed after it is read; handles share it.
struct CheckpointObject {
	vector<uint8_t> bytes;
	timestamp_t last_modified;
	string version_tag;
};

class ParquetCheckpointFileSystem;

class ParquetCheckpointFileHandle : public FileHandle {
public:
	ParquetCheckpointFileHandle(FileSystem &fs, string path, FileOpenFlags flags)
	    : FileHandle(fs, std::move(path), flags) {
	}
	~ParquetCheckpointFileHandle() override {
		ParquetCheckpointFileHandle::Close();
	}

	// One chunk of the manifest, opened on first touch. Scan threads
	// share this handle: the lock is held while the chunk is opened,
	// and the reads that follow take no lock of ours.
	struct Chunk {
		mutex lock;
		unique_ptr<CachingFileHandle> file;
	};

	shared_ptr<CheckpointObject> checkpoint; // the checkpoint object
	vector<CkptSegment> segments;
	idx_t file_size = 0;
	idx_t position = 0;
	FileSystem *raw_fs = nullptr;          // opens the chunks
	optional_ptr<FileOpener> opener;
	FileOpenFlags chunk_flags;             // how a chunk is opened
	vector<unique_ptr<Chunk>> chunks;      // one for each segment

	void Close() override {
		chunks.clear();
	}
};

class ParquetCheckpointFileSystem : public FileSystem {
public:
	explicit ParquetCheckpointFileSystem(DatabaseInstance &db_p)
	    : db(db_p), local_fs(FileSystem::CreateLocal()) {
	}

	std::string GetName() const override {
		return "ParquetCheckpointFileSystem";
	}

	bool CanHandleFile(const string &fpath) override {
		if (StringUtil::StartsWith(fpath, PQC_SCHEME)) {
			return true;
		}
		// bare .ckpt without a scheme = local; scheme'd paths must be
		// wrapped in pqc:// so we can never claim what another
		// filesystem (or we ourselves) should open
		return StringUtil::EndsWith(fpath, CKPT_SUFFIX) &&
		       fpath.find("://") == string::npos;
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener) override {
		if (flags.OpenForWriting()) {
			throw IOException(
			    "checkpoint filesystem is read-only: cannot write %s", path);
		}
		string raw = Unwrap(path);
		if (!StringUtil::EndsWith(raw, CKPT_SUFFIX)) {
			throw IOException(
			    "checkpoint filesystem: %s does not end in %s", raw,
			    CKPT_SUFFIX);
		}
		auto handle = make_uniq<ParquetCheckpointFileHandle>(*this, path, flags);
		handle->raw_fs = RawFsFor(raw);
		handle->opener = opener;
		// A chunk is read the way DuckDB reads a parquet file of its
		// own: the reader's request sizes, and for a remote chunk no
		// second buffer under the cache.
		handle->chunk_flags = FileFlags::FILE_FLAGS_READ;
		handle->chunk_flags.SetRequestSizing(flags.GetRequestSizing());
		if (FileSystem::IsRemoteFile(raw)) {
			handle->chunk_flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
		}

		// The checkpoint object, whole: the manifest, the rows not yet
		// in a chunk, the footer.
		handle->checkpoint = ReadCheckpoint(*handle->raw_fs, raw, opener);
		auto &blob = handle->checkpoint->bytes;

		char err[256] = {0};
		PqcReader *r = pqc_reader_open(blob.data(), blob.size(), err,
		                               sizeof(err));
		if (!r) {
			throw IOException("checkpoint %s: refused: %s", raw, err);
		}
		// the checkpoint records its own file name; the object is one
		// version of that file's checkpoint, <name>.<version>.ckpt
		// (<name>.ckpt before versions); the folder it is in is the root
		// chunk paths hang off
		string name = pqc_reader_name(r);
		if (!IsCheckpointOf(raw, name)) {
			pqc_reader_free(r);
			throw IOException(
			    "checkpoint %s: records name %s but the object is not a "
			    "checkpoint version of it (%s.<version>.ckpt)", raw, name,
			    name);
		}
		auto slash = raw.find_last_of('/');
		string root = slash == string::npos ? string() : raw.substr(0, slash + 1);

		idx_t off = 0;
		auto n = pqc_reader_n_chunks(r);
		for (uint32_t i = 0; i < n; i++) {
			const char *cname;
			uint64_t length;
			uint32_t crc;
			pqc_reader_chunk(r, i, &cname, &length, &crc);
			handle->segments.push_back(
			    CkptSegment {off, NumericCast<idx_t>(length),
			                 root + string(cname), 0});
			off += NumericCast<idx_t>(length);
		}
		uint64_t doff, dlen;
		pqc_reader_data(r, &doff, &dlen);
		handle->segments.push_back(CkptSegment {
		    off, NumericCast<idx_t>(dlen), string(),
		    NumericCast<idx_t>(doff)});
		handle->file_size = off + NumericCast<idx_t>(dlen);
		pqc_reader_free(r);
		for (idx_t i = 0; i < handle->segments.size(); i++) {
			handle->chunks.push_back(
			    make_uniq<ParquetCheckpointFileHandle::Chunk>());
		}
		return std::move(handle);
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
	          idx_t location) override {
		auto &h = handle.Cast<ParquetCheckpointFileHandle>();
		if (location + NumericCast<idx_t>(nr_bytes) > h.file_size) {
			throw IOException(
			    "checkpoint %s: read [%llu, %llu) beyond size %llu",
			    handle.GetPath(), location, location + nr_bytes,
			    h.file_size);
		}
		auto *out = static_cast<uint8_t *>(buffer);
		idx_t want_end = location + NumericCast<idx_t>(nr_bytes);
		for (idx_t i = 0; i < h.segments.size(); i++) {
			auto &seg = h.segments[i];
			idx_t seg_end = seg.start + seg.length;
			if (seg_end <= location || seg.start >= want_end) {
				continue;
			}
			idx_t lo = MaxValue(location, seg.start);
			idx_t hi = MinValue(want_end, seg_end);
			auto *dst = out + (lo - location);
			if (seg.path.empty()) {
				memcpy(dst,
				       h.checkpoint->bytes.data() + seg.blob_off +
				           (lo - seg.start),
				       hi - lo);
			} else {
				ChunkFile(h, i)
				    .Read(hi - lo, lo - seg.start)
				    .CopyTo(dst, hi - lo);
			}
		}
	}

	int64_t Read(FileHandle &handle, void *buffer,
	             int64_t nr_bytes) override {
		auto &h = handle.Cast<ParquetCheckpointFileHandle>();
		auto n = MinValue(NumericCast<idx_t>(nr_bytes),
		                  h.file_size - MinValue(h.position, h.file_size));
		if (n == 0) {
			return 0;
		}
		Read(handle, buffer, NumericCast<int64_t>(n), h.position);
		h.position += n;
		return NumericCast<int64_t>(n);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return NumericCast<int64_t>(
		    handle.Cast<ParquetCheckpointFileHandle>().file_size);
	}

	void Seek(FileHandle &handle, idx_t location) override {
		handle.Cast<ParquetCheckpointFileHandle>().position = location;
	}

	idx_t SeekPosition(FileHandle &handle) override {
		return handle.Cast<ParquetCheckpointFileHandle>().position;
	}

	void Reset(FileHandle &handle) override {
		handle.Cast<ParquetCheckpointFileHandle>().position = 0;
	}

	bool CanSeek() override {
		return true;
	}

	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		// the checkpoint object's own time, as it was opened: it
		// differs for every checkpoint, which is the signal a
		// parquet-metadata cache wants from this file
		return handle.Cast<ParquetCheckpointFileHandle>()
		    .checkpoint->last_modified;
	}

	string GetVersionTag(FileHandle &handle) override {
		return handle.Cast<ParquetCheckpointFileHandle>()
		    .checkpoint->version_tag;
	}

	bool FileExists(const string &filename,
	                optional_ptr<FileOpener> opener) override {
		string raw = Unwrap(filename);
		return RawFsFor(raw)->FileExists(raw, opener);
	}

	vector<OpenFileInfo> Glob(const string &path,
	                          FileOpener *opener) override {
		// no pattern support: a checkpoint is addressed exactly
		return {OpenFileInfo(path)};
	}

private:
	// Whether the object at `raw` is a checkpoint version of file `name`:
	// its base name is <name>.<6+ digits>.ckpt, or <name>.ckpt (a
	// checkpoint written before versions).
	static bool IsCheckpointOf(const string &raw, const string &name) {
		auto slash = raw.find_last_of('/');
		string base = slash == string::npos ? raw : raw.substr(slash + 1);
		if (base == name + CKPT_SUFFIX) {
			return true;
		}
		string head = name + ".";
		if (!StringUtil::StartsWith(base, head) ||
		    !StringUtil::EndsWith(base, CKPT_SUFFIX)) {
			return false;
		}
		string seq = base.substr(head.size(),
		                         base.size() - head.size() - strlen(CKPT_SUFFIX));
		if (seq.size() < 6) {
			return false;
		}
		for (char c : seq) {
			if (c < '0' || c > '9') {
				return false;
			}
		}
		return true;
	}

	// Whether `raw` names a checkpoint VERSION, <name>.parquet.<6+
	// digits>.ckpt: an object written once and never changed. A
	// checkpoint named without a version (written before versions) was
	// replaced in place, so its cache entry is checked at each open.
	static bool IsVersionedCheckpoint(const string &raw) {
		if (!StringUtil::EndsWith(raw, CKPT_SUFFIX)) {
			return false;
		}
		string stem = raw.substr(0, raw.size() - strlen(CKPT_SUFFIX));
		auto dot = stem.find_last_of('.');
		if (dot == string::npos || stem.size() - dot - 1 < 6) {
			return false;
		}
		for (idx_t i = dot + 1; i < stem.size(); i++) {
			if (stem[i] < '0' || stem[i] > '9') {
				return false;
			}
		}
		return StringUtil::EndsWith(stem.substr(0, dot), ".parquet");
	}

	// The checkpoint object at `raw`, whole. A version comes from the
	// few kept, or is read and kept; a checkpoint with no version in
	// its name is read at every open, because it was replaced in place.
	shared_ptr<CheckpointObject> ReadCheckpoint(FileSystem &raw_fs,
	                                            const string &raw,
	                                            optional_ptr<FileOpener> opener) {
		bool versioned = IsVersionedCheckpoint(raw);
		if (versioned) {
			lock_guard<mutex> guard(kept_lock);
			for (idx_t i = 0; i < kept.size(); i++) {
				if (kept[i].first == raw) {
					auto found = kept[i];       // the one read last is last
					kept.erase(kept.begin() + NumericCast<int64_t>(i));
					kept.push_back(found);
					return found.second;
				}
			}
		}
		auto file = raw_fs.OpenFile(raw, FileFlags::FILE_FLAGS_READ, opener);
		auto object = make_shared_ptr<CheckpointObject>();
		auto size = raw_fs.GetFileSize(*file);
		object->bytes.resize(NumericCast<size_t>(size));
		raw_fs.Read(*file, object->bytes.data(), size, 0);
		object->last_modified = raw_fs.GetLastModifiedTime(*file);
		object->version_tag = raw_fs.GetVersionTag(*file);
		if (versioned) {
			lock_guard<mutex> guard(kept_lock);
			kept.emplace_back(raw, object);
			if (kept.size() > KEPT_CHECKPOINTS) {
				kept.erase(kept.begin());
			}
		}
		return object;
	}

	// Open the chunk at `path` through DuckDB's caching file system. A
	// chunk is written once, so a cached copy is used with no validity
	// check and no request.
	unique_ptr<CachingFileHandle> OpenChunk(FileSystem &raw_fs, const string &path,
	                                        FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) {
		OpenFileInfo info(path);
		info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		info.extended_info->options["validate_external_file_cache"] =
		    Value::BOOLEAN(false);
		CachingFileSystem caching(raw_fs, db);
		return caching.OpenFile(
		    QueryContext(FileOpener::TryGetClientContext(opener)), info, flags,
		    opener);
	}

	static string Unwrap(const string &path) {
		if (StringUtil::StartsWith(path, PQC_SCHEME)) {
			return path.substr(strlen(PQC_SCHEME));
		}
		return path;
	}

	// which filesystem opens the RAW objects (ckpt + chunks): local
	// paths bypass the virtual filesystem entirely (we might be asked
	// first and must not recurse); scheme'd paths go through the
	// database's filesystems (httpfs etc.) — they can never come back
	// to us because we only claim scheme'd paths wrapped in pqc://.
	// The virtual filesystem itself, not db.GetFileSystem(): that one
	// is DuckDB's database-level wrapper, which supplies its own
	// opener and refuses ours (an internal error), and ours is the
	// query's, the one that carries the S3 secret.
	FileSystem *RawFsFor(const string &raw) {
		if (raw.find("://") == string::npos) {
			return local_fs.get();
		}
		return DBConfig::GetConfig(db).file_system.get();
	}

	// The caching handle of segment seg_idx's chunk, opened on first
	// touch. Any number of scan threads may ask at once.
	CachingFileHandle &ChunkFile(ParquetCheckpointFileHandle &h, idx_t seg_idx) {
		auto &chunk = *h.chunks[seg_idx];
		lock_guard<mutex> guard(chunk.lock);
		if (!chunk.file) {
			chunk.file = OpenChunk(*h.raw_fs, h.segments[seg_idx].path,
			                       h.chunk_flags, h.opener);
		}
		return *chunk.file;
	}

	// The checkpoint versions read last, the newest last.
	static constexpr idx_t KEPT_CHECKPOINTS = 8;
	mutex kept_lock;
	vector<std::pair<string, shared_ptr<CheckpointObject>>> kept;

	DatabaseInstance &db;
	unique_ptr<FileSystem> local_fs;
};

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	db.GetFileSystem().RegisterSubSystem(
	    make_uniq<ParquetCheckpointFileSystem>(db));
}

void ParquetCheckpointExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string ParquetCheckpointExtension::Name() {
	return "parquet_checkpoint";
}

std::string ParquetCheckpointExtension::Version() const {
#ifdef EXT_VERSION_PARQUET_CHECKPOINT
	return EXT_VERSION_PARQUET_CHECKPOINT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(parquet_checkpoint, loader) {
	duckdb::LoadInternal(loader);
}
}
