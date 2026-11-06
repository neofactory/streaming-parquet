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

#include "parquet_checkpoint_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

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

class ParquetCheckpointFileSystem;

class ParquetCheckpointFileHandle : public FileHandle {
public:
	ParquetCheckpointFileHandle(FileSystem &fs, string path, FileOpenFlags flags)
	    : FileHandle(fs, std::move(path), flags) {
	}
	~ParquetCheckpointFileHandle() override {
		ParquetCheckpointFileHandle::Close();
	}

	vector<uint8_t> blob;                  // the checkpoint object's bytes
	vector<CkptSegment> segments;
	idx_t file_size = 0;
	idx_t position = 0;
	FileSystem *raw_fs = nullptr;          // opens chunk objects
	optional_ptr<FileOpener> opener;
	// lazily-opened chunk handles, one per segment index
	unordered_map<idx_t, unique_ptr<FileHandle>> chunk_handles;

	void Close() override {
		chunk_handles.clear();
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

		// read the checkpoint object whole (it is small: manifest +
		// provisional tail + footer)
		auto ckpt_handle = handle->raw_fs->OpenFile(
		    raw, FileFlags::FILE_FLAGS_READ, opener);
		auto ckpt_size = handle->raw_fs->GetFileSize(*ckpt_handle);
		handle->blob.resize(NumericCast<size_t>(ckpt_size));
		handle->raw_fs->Read(*ckpt_handle, handle->blob.data(), ckpt_size, 0);

		char err[256] = {0};
		PqcReader *r = pqc_reader_open(handle->blob.data(),
		                               handle->blob.size(), err, sizeof(err));
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
				memcpy(dst, h.blob.data() + seg.blob_off + (lo - seg.start),
				       hi - lo);
			} else {
				auto &ch = ChunkHandle(h, i);
				h.raw_fs->Read(ch, dst,
				               NumericCast<int64_t>(hi - lo),
				               lo - seg.start);
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
		// the ckpt object's own mtime: it advances on every
		// checkpoint, which is exactly the cache-invalidation signal
		// a parquet-metadata cache wants from this file
		auto &h = handle.Cast<ParquetCheckpointFileHandle>();
		string raw = Unwrap(handle.GetPath());
		auto ch = h.raw_fs->OpenFile(raw, FileFlags::FILE_FLAGS_READ,
		                             h.opener);
		return h.raw_fs->GetLastModifiedTime(*ch);
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

	FileHandle &ChunkHandle(ParquetCheckpointFileHandle &h, idx_t seg_idx) {
		auto it = h.chunk_handles.find(seg_idx);
		if (it == h.chunk_handles.end()) {
			auto ch = h.raw_fs->OpenFile(h.segments[seg_idx].path,
			                             FileFlags::FILE_FLAGS_READ,
			                             h.opener);
			it = h.chunk_handles.emplace(seg_idx, std::move(ch)).first;
		}
		return *it->second;
	}

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
