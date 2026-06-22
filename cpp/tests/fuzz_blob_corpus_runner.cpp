/*
** fuzz_blob_corpus_runner.cpp — Standalone driver that feeds corpus files
** through fuzz_msgpack_blob.cpp, without requiring libFuzzer.
**
** Usage: fuzz_blob_corpus_runner <corpus_dir>
**
** Reads every file in <corpus_dir>, loads its bytes, and calls
** LLVMFuzzerTestOneInput().  Reports any failures.
*/
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>

/* Provided by fuzz_msgpack_blob.cpp (compiled together) */
extern "C" int LLVMFuzzerInitialize(int *argc, char ***argv);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

static uint8_t *read_file(const char *path, size_t *out_size) {
    FILE *f = std::fopen(path, "rb");
    if (!f) return nullptr;
    std::fseek(f, 0, SEEK_END);
    long len = std::ftell(f);
    std::fseek(f, 0, SEEK_SET);
    if (len < 0) { std::fclose(f); return nullptr; }
    auto *buf = static_cast<uint8_t *>(std::malloc(static_cast<size_t>(len) + 1));
    if (!buf) { std::fclose(f); return nullptr; }
    size_t nread = std::fread(buf, 1, static_cast<size_t>(len), f);
    std::fclose(f);
    *out_size = nread;
    return buf;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <corpus_dir>\n", argv[0]);
        return 1;
    }

    LLVMFuzzerInitialize(&argc, &argv);

    DIR *d = opendir(argv[1]);
    if (!d) {
        std::fprintf(stderr, "Cannot open directory: %s\n", argv[1]);
        return 1;
    }

    int total = 0, ok = 0, fail = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != nullptr) {
        if (ent->d_name[0] == '.') continue;

        char path[4096];
        std::snprintf(path, sizeof(path), "%s/%s", argv[1], ent->d_name);

        struct stat st;
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;

        size_t sz = 0;
        uint8_t *buf = read_file(path, &sz);
        if (!buf) {
            std::fprintf(stderr, "  SKIP  %s (read error)\n", ent->d_name);
            continue;
        }

        total++;
        int rc = LLVMFuzzerTestOneInput(buf, sz);
        if (rc == 0) {
            ok++;
        } else {
            std::fprintf(stderr, "  FAIL  %s (rc=%d)\n", ent->d_name, rc);
            fail++;
        }
        std::free(buf);
    }
    closedir(d);

    std::printf("%d passed, %d failed out of %d\n", ok, fail, total);
    return fail > 0 ? 1 : 0;
}
