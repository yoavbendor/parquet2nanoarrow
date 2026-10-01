/* SPDX-License-Identifier: Apache-2.0
 * Copyright (c) 2026 Yoav Bendor
 *
 * parquet2nanoarrow C ABI: open a Parquet file as an Arrow C stream (one struct batch per row
 * group). For non-C++ consumers (ctypes/cffi, other languages).
 */
#ifndef PARQUET2NANOARROW_H_INCLUDED
#define PARQUET2NANOARROW_H_INCLUDED

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArrayStream;

/* Open `path` and export it to `out`. `columns` (may be NULL) selects top-level columns by name;
 * `skip_unsupported` drops columns this version cannot decode from the default projection.
 * Returns 0 on success; otherwise non-zero with a NUL-terminated message in `err` (if non-NULL). */
int p2n_open_stream(const char* path, const char* const* columns, int n_columns, int skip_unsupported,
                    struct ArrowArrayStream* out, char* err, size_t err_len);

#ifdef __cplusplus
}
#endif

#endif /* PARQUET2NANOARROW_H_INCLUDED */
