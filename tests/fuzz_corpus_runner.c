/*
** fuzz_corpus_runner.c — Standalone driver that feeds corpus files through the
** same code paths as fuzz_msgpack.c, without requiring libFuzzer.
**
** Usage: fuzz_corpus_runner <corpus_dir>
**
** Reads every file in <corpus_dir>, loads its bytes, and calls
** LLVMFuzzerTestOneInput().  Reports any failures.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <stdint.h>

/* Provided by fuzz_msgpack.c (compiled together) */
extern int LLVMFuzzerInitialize(int *argc, char ***argv);
extern int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

static uint8_t *read_file(const char *path, size_t *out_size) {
  FILE *f = fopen(path, "rb");
  if (!f) return NULL;
  fseek(f, 0, SEEK_END);
  long len = ftell(f);
  fseek(f, 0, SEEK_SET);
  if (len < 0) { fclose(f); return NULL; }
  uint8_t *buf = (uint8_t *)malloc((size_t)len + 1);
  if (!buf) { fclose(f); return NULL; }
  size_t nread = fread(buf, 1, (size_t)len, f);
  fclose(f);
  *out_size = nread;
  return buf;
}

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <corpus_dir>\n", argv[0]);
    return 1;
  }

  LLVMFuzzerInitialize(&argc, &argv);

  DIR *d = opendir(argv[1]);
  if (!d) {
    fprintf(stderr, "Cannot open directory: %s\n", argv[1]);
    return 1;
  }

  int total = 0, ok = 0, fail = 0;
  struct dirent *ent;
  while ((ent = readdir(d)) != NULL) {
    if (ent->d_name[0] == '.') continue;

    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", argv[1], ent->d_name);

    struct stat st;
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;

    size_t sz = 0;
    uint8_t *buf = read_file(path, &sz);
    if (!buf) {
      fprintf(stderr, "  SKIP  %s (read error)\n", ent->d_name);
      continue;
    }

    total++;
    int rc = LLVMFuzzerTestOneInput(buf, sz);
    if (rc == 0) {
      ok++;
    } else {
      fprintf(stderr, "  FAIL  %s (rc=%d)\n", ent->d_name, rc);
      fail++;
    }
    free(buf);
  }
  closedir(d);

  /* Also test degenerate inputs */
  total++; if (LLVMFuzzerTestOneInput(NULL, 0) == 0) ok++; else fail++;
  uint8_t one = 0xFF;
  total++; if (LLVMFuzzerTestOneInput(&one, 1) == 0) ok++; else fail++;
  uint8_t zeros[64]; memset(zeros, 0, sizeof(zeros));
  total++; if (LLVMFuzzerTestOneInput(zeros, sizeof(zeros)) == 0) ok++; else fail++;
  uint8_t ones[64]; memset(ones, 0xFF, sizeof(ones));
  total++; if (LLVMFuzzerTestOneInput(ones, sizeof(ones)) == 0) ok++; else fail++;

  printf("Corpus runner: %d inputs, %d ok, %d failed\n", total, ok, fail);
  return fail > 0 ? 1 : 0;
}
