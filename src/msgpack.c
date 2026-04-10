/*
** SQLite MessagePack Extension
**
** This file implements SQL functions for creating, querying, and manipulating
** MessagePack binary data (stored as BLOBs), mirroring the JSON1 extension API.
**
** Functions provided (Phase 1):
**   msgpack_valid(mp)          -- 1 if well-formed msgpack, 0 otherwise
**   msgpack_quote(value)       -- encode a single SQL value as msgpack BLOB
**
** Build as loadable extension:
**   gcc -shared -fPIC -o msgpack.dylib ext/msgpack/msgpack.c -I.
**
** Usage in SQLite shell:
**   .load ./msgpack
**   SELECT hex(msgpack_quote(42));
**   SELECT msgpack_valid(msgpack_quote(42));
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#define SQLITE_MSGPACK_VERSION       "1.0.1"
#define SQLITE_MSGPACK_VERSION_NUMBER 1000001  /* major*1000000 + minor*1000 + patch */

#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <stdlib.h>

/* Unsigned integer types */
typedef unsigned char  u8;
typedef unsigned short u16;
typedef unsigned int   u32;
typedef sqlite3_int64  i64;
typedef sqlite3_uint64 u64;

/* Maximum nesting depth for recursive operations (JSON conversion, merge-patch) */
#define MP_MAX_DEPTH 200

/* Maximum output buffer size to prevent runaway allocation on malformed input */
#define MP_MAX_OUTPUT (64*1024*1024)

/*
** MessagePack format constants
*/
#define MP_NIL         0xc0
#define MP_FALSE       0xc2
#define MP_TRUE        0xc3
#define MP_BIN8        0xc4
#define MP_BIN16       0xc5
#define MP_BIN32       0xc6
#define MP_EXT8        0xc7
#define MP_EXT16       0xc8
#define MP_EXT32       0xc9
#define MP_FLOAT32     0xca
#define MP_FLOAT64     0xcb
#define MP_UINT8       0xcc
#define MP_UINT16      0xcd
#define MP_UINT32      0xce
#define MP_UINT64      0xcf
#define MP_INT8        0xd0
#define MP_INT16       0xd1
#define MP_INT32       0xd2
#define MP_INT64       0xd3
#define MP_FIXEXT1     0xd4
#define MP_FIXEXT2     0xd5
#define MP_FIXEXT4     0xd6
#define MP_FIXEXT8     0xd7
#define MP_FIXEXT16    0xd8
#define MP_STR8        0xd9
#define MP_STR16       0xda
#define MP_STR32       0xdb
#define MP_ARRAY16     0xdc
#define MP_ARRAY32     0xdd
#define MP_MAP16       0xde
#define MP_MAP32       0xdf

/* Masks for "fix" types */
#define MP_FIXMAP_MASK    0x80   /* 0x80-0x8f */
#define MP_FIXARRAY_MASK  0x90   /* 0x90-0x9f */
#define MP_FIXSTR_MASK    0xa0   /* 0xa0-0xbf */
#define MP_NEGFIXINT_MASK 0xe0   /* 0xe0-0xff */
#define MP_POSFIXINT_MAX  0x7f

/*
** ============================================================
** Big-endian byte-order helpers (no htonl/ntohl dependency)
** ============================================================
*/
static u16 mpRead16(const u8 *p){
  return (u16)(((u16)p[0]<<8)|p[1]);
}
static u32 mpRead32(const u8 *p){
  return ((u32)p[0]<<24)|((u32)p[1]<<16)|((u32)p[2]<<8)|p[3];
}
static u64 mpRead64(const u8 *p){
  return ((u64)mpRead32(p)<<32)|(u64)mpRead32(p+4);
}
static void mpWrite16(u8 *p, u16 v){
  p[0]=(u8)(v>>8); p[1]=(u8)v;
}
static void mpWrite32(u8 *p, u32 v){
  p[0]=(u8)(v>>24); p[1]=(u8)(v>>16); p[2]=(u8)(v>>8); p[3]=(u8)v;
}
static void mpWrite64(u8 *p, u64 v){
  mpWrite32(p,(u32)(v>>32)); mpWrite32(p+4,(u32)v);
}

/*
** ============================================================
** MpBuf — growable output buffer (stack-allocated until it grows)
** ============================================================
*/
#define MPBUF_INITSIZE 256

typedef struct MpBuf {
  u8  *aBuf;           /* current buffer pointer (aSpace or heap) */
  u32  nUsed;          /* bytes written */
  u32  nAlloc;         /* bytes allocated */
  u8   bErr;           /* error flag */
  sqlite3_context *pCtx; /* for sqlite3_result_error_nomem */
  u8   aSpace[MPBUF_INITSIZE]; /* initial stack storage */
} MpBuf;

static void mpBufInit(MpBuf *p, sqlite3_context *pCtx){
  p->aBuf   = p->aSpace;
  p->nUsed  = 0;
  p->nAlloc = MPBUF_INITSIZE;
  p->bErr   = 0;
  p->pCtx   = pCtx;
}

static void mpBufReset(MpBuf *p){
  if( p->aBuf != p->aSpace ){
    sqlite3_free(p->aBuf);
    p->aBuf   = p->aSpace;
    p->nAlloc = MPBUF_INITSIZE;
  }
  p->nUsed = 0;
  p->bErr  = 0;
}

static int mpBufGrow(MpBuf *p, u32 nNeeded){
  u32 nNew;
  u8 *aNew;
  if( p->bErr ) return 0;
  if( nNeeded > MP_MAX_OUTPUT ){
    p->bErr = 1;
    if(p->pCtx) sqlite3_result_error(p->pCtx, "msgpack output too large", -1);
    return 0;
  }
  nNew = p->nAlloc;
  while( nNew < nNeeded ) nNew *= 2;
  if( p->aBuf == p->aSpace ){
    aNew = sqlite3_malloc(nNew);
    if( !aNew ){ p->bErr=1; if(p->pCtx) sqlite3_result_error_nomem(p->pCtx); return 0; }
    memcpy(aNew, p->aSpace, p->nUsed);
  } else {
    aNew = sqlite3_realloc(p->aBuf, nNew);
    if( !aNew ){ p->bErr=1; if(p->pCtx) sqlite3_result_error_nomem(p->pCtx); return 0; }
  }
  p->aBuf   = aNew;
  p->nAlloc = nNew;
  return 1;
}

static void mpBufAppend(MpBuf *p, const u8 *pData, u32 n){
  if( p->bErr ) return;
  if( n > MP_MAX_OUTPUT - p->nUsed ){ p->bErr=1; return; }
  if( n > p->nAlloc - p->nUsed ){
    if( !mpBufGrow(p, p->nUsed + n) ) return;
  }
  memcpy(p->aBuf + p->nUsed, pData, n);
  p->nUsed += n;
}

static void mpBufAppend1(MpBuf *p, u8 b){
  if( p->bErr ) return;
  if( p->nUsed >= MP_MAX_OUTPUT ){ p->bErr=1; return; }
  if( p->nUsed + 1 > p->nAlloc ){
    if( !mpBufGrow(p, p->nUsed + 1) ) return;
  }
  p->aBuf[p->nUsed++] = b;
}

/* Reserve N bytes and return a pointer to them, or NULL on OOM */
static u8 *mpBufReserve(MpBuf *p, u32 n){
  u8 *ret;
  if( p->bErr ) return 0;
  if( n > MP_MAX_OUTPUT - p->nUsed ){ p->bErr=1; return 0; }
  if( n > p->nAlloc - p->nUsed ){
    if( !mpBufGrow(p, p->nUsed + n) ) return 0;
  }
  ret = p->aBuf + p->nUsed;
  p->nUsed += n;
  return ret;
}

/* Transfer ownership of the buffer to the caller (caller must sqlite3_free) */
static u8 *mpBufFinish(MpBuf *p, u32 *pnOut){
  u8 *result;
  *pnOut = p->nUsed;
  if( p->bErr ) return 0;
  if( p->aBuf == p->aSpace ){
    result = sqlite3_malloc(p->nUsed ? p->nUsed : 1);
    if( !result ){ if(p->pCtx) sqlite3_result_error_nomem(p->pCtx); return 0; }
    memcpy(result, p->aSpace, p->nUsed);
  } else {
    result = p->aBuf;
    p->aBuf   = p->aSpace;
    p->nAlloc = MPBUF_INITSIZE;
  }
  p->nUsed = 0;
  return result;
}

/*
** ============================================================
** Encode a single SQL value into p as msgpack
** ============================================================
*/

/* Forward declaration — defined after mpSkipOne below */
static int mpIsValid(const u8 *a, u32 n);

static void mpEncodeSqlValue(MpBuf *p, sqlite3_value *v){
  int t = sqlite3_value_type(v);
  switch( t ){
    case SQLITE_NULL: {
      mpBufAppend1(p, MP_NIL);
      break;
    }
    case SQLITE_INTEGER: {
      i64 x = sqlite3_value_int64(v);
      if( x >= 0 ){
        if( x <= 0x7f ){
          mpBufAppend1(p, (u8)x);              /* positive fixint */
        } else if( x <= 0xff ){
          u8 b[2] = { MP_UINT8, (u8)x };
          mpBufAppend(p, b, 2);
        } else if( x <= 0xffff ){
          u8 b[3]; b[0]=MP_UINT16; mpWrite16(b+1,(u16)x);
          mpBufAppend(p, b, 3);
        } else if( x <= (i64)0xffffffff ){
          u8 b[5]; b[0]=MP_UINT32; mpWrite32(b+1,(u32)x);
          mpBufAppend(p, b, 5);
        } else {
          u8 b[9]; b[0]=MP_UINT64; mpWrite64(b+1,(u64)x);
          mpBufAppend(p, b, 9);
        }
      } else {
        if( x >= -32 ){
          mpBufAppend1(p, (u8)(i64)x);         /* negative fixint */
        } else if( x >= -128 ){
          u8 b[2] = { MP_INT8, (u8)(i64)x };
          mpBufAppend(p, b, 2);
        } else if( x >= -32768 ){
          u8 b[3]; b[0]=MP_INT16; mpWrite16(b+1,(u16)(i64)x);
          mpBufAppend(p, b, 3);
        } else if( x >= (i64)-2147483648LL ){
          u8 b[5]; b[0]=MP_INT32; mpWrite32(b+1,(u32)(i64)x);
          mpBufAppend(p, b, 5);
        } else {
          u8 b[9]; b[0]=MP_INT64; mpWrite64(b+1,(u64)(i64)x);
          mpBufAppend(p, b, 9);
        }
      }
      break;
    }
    case SQLITE_FLOAT: {
      double d = sqlite3_value_double(v);
      u8 b[9];
      u64 bits;
      b[0] = MP_FLOAT64;
      memcpy(&bits, &d, 8);
      mpWrite64(b+1, bits);
      mpBufAppend(p, b, 9);
      break;
    }
    case SQLITE_TEXT: {
      const u8 *z = (const u8*)sqlite3_value_text(v);
      u32 n = z ? (u32)sqlite3_value_bytes(v) : 0;
      if( n <= 31 ){
        mpBufAppend1(p, (u8)(MP_FIXSTR_MASK | n));
      } else if( n <= 0xff ){
        u8 b[2] = { MP_STR8, (u8)n };
        mpBufAppend(p, b, 2);
      } else if( n <= 0xffff ){
        u8 b[3]; b[0]=MP_STR16; mpWrite16(b+1,(u16)n);
        mpBufAppend(p, b, 3);
      } else {
        u8 b[5]; b[0]=MP_STR32; mpWrite32(b+1,n);
        mpBufAppend(p, b, 5);
      }
      if( z ) mpBufAppend(p, z, n);
      break;
    }
    case SQLITE_BLOB: {
      const u8 *z = (const u8*)sqlite3_value_blob(v);
      u32 n = z ? (u32)sqlite3_value_bytes(v) : 0;
      /* If this BLOB is itself valid msgpack, embed it directly as a nested
      ** element (equivalent to json1's json() wrapper, but automatic).
      ** Otherwise encode as msgpack bin type for raw binary data. */
      if( z && n > 0 && mpIsValid(z, n) ){
        mpBufAppend(p, z, n);
      } else {
        if( n <= 0xff ){
          u8 b[2] = { MP_BIN8, (u8)n };
          mpBufAppend(p, b, 2);
        } else if( n <= 0xffff ){
          u8 b[3]; b[0]=MP_BIN16; mpWrite16(b+1,(u16)n);
          mpBufAppend(p, b, 3);
        } else {
          u8 b[5]; b[0]=MP_BIN32; mpWrite32(b+1,n);
          mpBufAppend(p, b, 5);
        }
        if( z ) mpBufAppend(p, z, n);
      }
      break;
    }
  }
}

/*
** ============================================================
** mpSkipOne — skip one complete msgpack element at offset i
** in blob (a, n). Returns offset of next element, or 0 on error.
** ============================================================
*/
static u32 mpSkipOne(const u8 *a, u32 n, u32 i){
  u32 count, j;
  if( i >= n ) return 0;
  u8 b = a[i++];

  /* positive fixint */
  if( b <= 0x7f ) return i;
  /* negative fixint */
  if( b >= 0xe0 ) return i;

  switch( b ){
    /* fixed-length scalars */
    case MP_NIL: case MP_FALSE: case MP_TRUE:
      return i;
    case MP_FLOAT32:
      return (i+4 <= n) ? i+4 : 0;
    case MP_FLOAT64: case MP_INT64: case MP_UINT64:
      return (i+8 <= n) ? i+8 : 0;
    case MP_UINT8: case MP_INT8:
      return (i+1 <= n) ? i+1 : 0;
    case MP_UINT16: case MP_INT16:
      return (i+2 <= n) ? i+2 : 0;
    case MP_UINT32: case MP_INT32:
      return (i+4 <= n) ? i+4 : 0;

    /* bin */
    case MP_BIN8:
      if( i+1 > n ) return 0;
      { u32 sz = a[i]; i++; return (sz<=n-i)?i+sz:0; }
    case MP_BIN16:
      if( i+2 > n ) return 0;
      { u32 sz = mpRead16(a+i); i+=2; return (sz<=n-i)?i+sz:0; }
    case MP_BIN32:
      if( i+4 > n ) return 0;
      { u32 sz = mpRead32(a+i); i+=4; return (sz<=n-i)?i+sz:0; }

    /* str */
    case MP_STR8:
      if( i+1 > n ) return 0;
      { u32 sz = a[i]; i++; return (sz<=n-i)?i+sz:0; }
    case MP_STR16:
      if( i+2 > n ) return 0;
      { u32 sz = mpRead16(a+i); i+=2; return (sz<=n-i)?i+sz:0; }
    case MP_STR32:
      if( i+4 > n ) return 0;
      { u32 sz = mpRead32(a+i); i+=4; return (sz<=n-i)?i+sz:0; }

    /* fixext */
    case MP_FIXEXT1:  return (i+2<=n)?i+2:0;
    case MP_FIXEXT2:  return (i+3<=n)?i+3:0;
    case MP_FIXEXT4:  return (i+5<=n)?i+5:0;
    case MP_FIXEXT8:  return (i+9<=n)?i+9:0;
    case MP_FIXEXT16: return (i+17<=n)?i+17:0;

    /* ext */
    case MP_EXT8:
      if( i+2 > n ) return 0;
      { u32 sz = a[i]; i+=2; return (sz<=n-i)?i+sz:0; } /* skip type byte */
    case MP_EXT16:
      if( i+3 > n ) return 0;
      { u32 sz = mpRead16(a+i); i+=3; return (sz<=n-i)?i+sz:0; }
    case MP_EXT32:
      if( i+5 > n ) return 0;
      { u32 sz = mpRead32(a+i); i+=5; return (sz<=n-i)?i+sz:0; }

    /* array16 */
    case MP_ARRAY16:
      if( i+2 > n ) return 0;
      count = mpRead16(a+i); i+=2;
      for( j=0; j<count; j++ ){ i = mpSkipOne(a,n,i); if(!i&&j<count-1) return 0; }
      return i;
    /* array32 */
    case MP_ARRAY32:
      if( i+4 > n ) return 0;
      count = mpRead32(a+i); i+=4;
      for( j=0; j<count; j++ ){ i = mpSkipOne(a,n,i); if(!i&&j<count-1) return 0; }
      return i;
    /* map16 */
    case MP_MAP16:
      if( i+2 > n ) return 0;
      count = mpRead16(a+i); i+=2;
      for( j=0; j<count*2; j++ ){ i = mpSkipOne(a,n,i); if(!i&&j<count*2-1) return 0; }
      return i;
    /* map32 */
    case MP_MAP32:
      if( i+4 > n ) return 0;
      count = mpRead32(a+i); i+=4;
      for( j=0; j<count*2; j++ ){ i = mpSkipOne(a,n,i); if(!i&&j<count*2-1) return 0; }
      return i;

    default:
      /* 0xc1 is unused / invalid */
      if( b == 0xc1 ) return 0;
      break;
  }

  /* fixmap: 0x80-0x8f */
  if( (b & 0xf0) == 0x80 ){
    count = b & 0x0f;
    for( j=0; j<count*2; j++ ){ i = mpSkipOne(a,n,i); if(!i&&j<count*2-1) return 0; }
    return i;
  }
  /* fixarray: 0x90-0x9f */
  if( (b & 0xf0) == 0x90 ){
    count = b & 0x0f;
    for( j=0; j<count; j++ ){ i = mpSkipOne(a,n,i); if(!i&&j<count-1) return 0; }
    return i;
  }
  /* fixstr: 0xa0-0xbf */
  if( (b & 0xe0) == 0xa0 ){
    u32 sz = b & 0x1f;
    return (i+sz<=n)?i+sz:0;
  }
  return 0;
}

/*
** mpIsValid — return 1 if (a,n) contains exactly one well-formed msgpack element
*/
static int mpIsValid(const u8 *a, u32 n){
  u32 end;
  if( n == 0 ) return 0;
  end = mpSkipOne(a, n, 0);
  return (end == n) ? 1 : 0;
}

/*
** ============================================================
** Phase 2: Array / map header encoders
** ============================================================
*/
static void mpEncodeArrayHeader(MpBuf *p, u32 count){
  if( count <= 15 ){
    mpBufAppend1(p, (u8)(MP_FIXARRAY_MASK | count));
  } else if( count <= 0xffff ){
    u8 b[3]; b[0]=MP_ARRAY16; mpWrite16(b+1,(u16)count);
    mpBufAppend(p, b, 3);
  } else {
    u8 b[5]; b[0]=MP_ARRAY32; mpWrite32(b+1,count);
    mpBufAppend(p, b, 5);
  }
}

static void mpEncodeMapHeader(MpBuf *p, u32 count){
  if( count <= 15 ){
    mpBufAppend1(p, (u8)(MP_FIXMAP_MASK | count));
  } else if( count <= 0xffff ){
    u8 b[3]; b[0]=MP_MAP16; mpWrite16(b+1,(u16)count);
    mpBufAppend(p, b, 3);
  } else {
    u8 b[5]; b[0]=MP_MAP32; mpWrite32(b+1,count);
    mpBufAppend(p, b, 5);
  }
}

/*
** ============================================================
** Phase 3: Type helpers, path parsing, lookup, element return
** ============================================================
*/

/* Return type name string for element at offset i in (a,n) */
static const char *mpGetTypeStr(const u8 *a, u32 n, u32 i){
  u8 b;
  if( i >= n ) return "null";
  b = a[i];
  if( b <= 0x7f ) return "integer";
  if( b >= 0xe0 ) return "integer";
  if( b >= 0x80 && b <= 0x8f ) return "map";
  if( b >= 0x90 && b <= 0x9f ) return "array";
  if( b >= 0xa0 && b <= 0xbf ) return "text";
  switch( b ){
    case MP_NIL:    return "null";
    case MP_FALSE:  return "false";
    case MP_TRUE:   return "true";
    case MP_UINT8:  case MP_UINT16: case MP_UINT32: case MP_UINT64:
    case MP_INT8:   case MP_INT16:  case MP_INT32:  case MP_INT64:
      return "integer";
    case MP_FLOAT32: case MP_FLOAT64: return "real";
    case MP_STR8:  case MP_STR16:  case MP_STR32:  return "text";
    case MP_BIN8:  case MP_BIN16:  case MP_BIN32:  return "blob";
    case MP_ARRAY16: case MP_ARRAY32: return "array";
    case MP_MAP16:   case MP_MAP32:   return "map";
    case MP_EXT8:    case MP_EXT16:   case MP_EXT32:
    case MP_FIXEXT1: case MP_FIXEXT2: case MP_FIXEXT4:
    case MP_FIXEXT8: case MP_FIXEXT16: return "ext";
    default: return "null";
  }
}

/* Return element count for array or map at offset i; -1 if not a container */
static i64 mpGetContainerCount(const u8 *a, u32 n, u32 i){
  u8 b;
  if( i >= n ) return -1;
  b = a[i];
  if( b >= 0x90 && b <= 0x9f ) return (i64)(b & 0x0f);
  if( b >= 0x80 && b <= 0x8f ) return (i64)(b & 0x0f);
  if( b == MP_ARRAY16 && i+3 <= n ) return (i64)mpRead16(a+i+1);
  if( b == MP_ARRAY32 && i+5 <= n ) return (i64)mpRead32(a+i+1);
  if( b == MP_MAP16   && i+3 <= n ) return (i64)mpRead16(a+i+1);
  if( b == MP_MAP32   && i+5 <= n ) return (i64)mpRead32(a+i+1);
  return -1;
}

/*
** Parse one path component from zPath at position *pi.
** On 'k': sets *pKey, *nKey.  On 'i': sets *pIdx.
** Returns 'k', 'i', 0 (end of path), or -1 (parse error).
*/
static int mpPathStep(
  const char *zPath, int *pi,
  const char **pKey, int *nKey,
  i64 *pIdx
){
  int i = *pi;
  if( zPath[i]=='\0' ) return 0;
  if( zPath[i]=='.' ){
    int start;
    i++;
    start = i;
    while( zPath[i] && zPath[i]!='.' && zPath[i]!='[' ) i++;
    *pKey = zPath + start;
    *nKey = i - start;
    *pi   = i;
    return 'k';
  }
  if( zPath[i]=='[' ){
    i64 idx = 0;
    int hasDigit = 0;
    i++;
    while( zPath[i]>='0' && zPath[i]<='9' ){
      idx = idx*10 + (zPath[i]-'0');
      i++;
      hasDigit = 1;
    }
    if( !hasDigit || zPath[i]!=']' ) return -1;
    i++;
    *pIdx = idx;
    *pi   = i;
    return 'i';
  }
  return -1;
}

/*
** Resolve path zPath in blob (a,n) starting at byte iRoot.
** On success: sets *piStart, *piEnd and returns SQLITE_OK.
** Returns SQLITE_NOTFOUND if no such element, SQLITE_ERROR on bad path/blob.
*/
static int mpLookup(
  const u8 *a, u32 n, u32 iRoot,
  const char *zPath,
  u32 *piStart, u32 *piEnd
){
  int pi;
  u32 iCur = iRoot;
  if( !zPath || zPath[0]!='$' ) return SQLITE_ERROR;
  pi = 1; /* skip '$' */

  for( ;; ){
    const char *zKey = 0;
    int nKey = 0;
    i64 idx = 0;
    int step = mpPathStep(zPath, &pi, &zKey, &nKey, &idx);

    if( step == 0 ){
      u32 iNext = mpSkipOne(a, n, iCur);
      *piStart = iCur;
      *piEnd   = iNext ? iNext : n;
      return (iNext || iCur==n) ? SQLITE_OK : SQLITE_ERROR;
    }
    if( step < 0 ) return SQLITE_ERROR;
    if( iCur >= n ) return SQLITE_NOTFOUND;

    if( step == 'i' ){
      /* Array index */
      u8 b = a[iCur];
      u32 count, elemOff;
      i64 j;
      if( b >= 0x90 && b <= 0x9f ){
        count = b & 0x0f; elemOff = iCur+1;
      } else if( b == MP_ARRAY16 ){
        if( iCur+3 > n ) return SQLITE_ERROR;
        count = mpRead16(a+iCur+1); elemOff = iCur+3;
      } else if( b == MP_ARRAY32 ){
        if( iCur+5 > n ) return SQLITE_ERROR;
        count = mpRead32(a+iCur+1); elemOff = iCur+5;
      } else {
        return SQLITE_NOTFOUND;
      }
      if( idx < 0 || (u64)idx >= (u64)count ) return SQLITE_NOTFOUND;
      iCur = elemOff;
      for( j=0; j<idx; j++ ){
        iCur = mpSkipOne(a, n, iCur);
        if( !iCur ) return SQLITE_ERROR;
      }

    } else { /* 'k' — map key */
      u8 b = a[iCur];
      u32 count, elemOff, j;
      int found = 0;
      if( b >= 0x80 && b <= 0x8f ){
        count = b & 0x0f; elemOff = iCur+1;
      } else if( b == MP_MAP16 ){
        if( iCur+3 > n ) return SQLITE_ERROR;
        count = mpRead16(a+iCur+1); elemOff = iCur+3;
      } else if( b == MP_MAP32 ){
        if( iCur+5 > n ) return SQLITE_ERROR;
        count = mpRead32(a+iCur+1); elemOff = iCur+5;
      } else {
        return SQLITE_NOTFOUND;
      }
      iCur = elemOff;
      for( j=0; j<count && !found; j++ ){
        u8 kb;
        const char *kStr = 0;
        u32 kLen = 0, valOff;
        if( iCur >= n ) return SQLITE_ERROR;
        kb = a[iCur];
        if( kb >= 0xa0 && kb <= 0xbf ){
          kLen = kb & 0x1f; kStr = (const char*)(a+iCur+1);
        } else if( kb == MP_STR8  && iCur+2 <= n ){
          kLen = a[iCur+1]; kStr = (const char*)(a+iCur+2);
        } else if( kb == MP_STR16 && iCur+3 <= n ){
          kLen = mpRead16(a+iCur+1); kStr = (const char*)(a+iCur+3);
        } else if( kb == MP_STR32 && iCur+5 <= n ){
          kLen = mpRead32(a+iCur+1); kStr = (const char*)(a+iCur+5);
        }
        valOff = mpSkipOne(a, n, iCur);
        if( !valOff ) return SQLITE_ERROR;
        if( kStr && (int)kLen==nKey && memcmp(kStr,zKey,(size_t)nKey)==0 ){
          iCur = valOff;
          found = 1;
        } else {
          iCur = mpSkipOne(a, n, valOff);
          if( !iCur ) return SQLITE_ERROR;
        }
      }
      if( !found ) return SQLITE_NOTFOUND;
    }
  } /* for(;;) */
}

/*
** Return the msgpack element at (a[iStart..iEnd]) as an appropriate SQL value:
**   scalars → int / real / text / null
**   containers (array, map) and bin/ext → BLOB
*/
static void mpReturnElement(
  sqlite3_context *ctx,
  const u8 *a, u32 n,
  u32 iStart, u32 iEnd
){
  u8 b;
  if( iStart >= n || iStart >= iEnd ){ sqlite3_result_null(ctx); return; }
  b = a[iStart];

  if( b == MP_NIL   ){ sqlite3_result_null(ctx); return; }
  if( b == MP_FALSE ){ sqlite3_result_int(ctx, 0); return; }
  if( b == MP_TRUE  ){ sqlite3_result_int(ctx, 1); return; }
  if( b <= 0x7f     ){ sqlite3_result_int(ctx, (int)b); return; }
  if( b >= 0xe0     ){ sqlite3_result_int(ctx, (int)(signed char)b); return; }

  switch( b ){
    case MP_UINT8:
      if( iStart+2 <= n ) sqlite3_result_int64(ctx, (i64)a[iStart+1]);
      break;
    case MP_UINT16:
      if( iStart+3 <= n ) sqlite3_result_int64(ctx, (i64)mpRead16(a+iStart+1));
      break;
    case MP_UINT32:
      if( iStart+5 <= n ) sqlite3_result_int64(ctx, (i64)mpRead32(a+iStart+1));
      break;
    case MP_UINT64: {
      if( iStart+9 <= n ){
        u64 v = mpRead64(a+iStart+1);
        if( v <= (u64)9223372036854775807ULL ){
          sqlite3_result_int64(ctx, (i64)v);
        } else {
          char buf[24];
          sqlite3_snprintf(24, buf, "%llu", (unsigned long long)v);
          sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
        }
      }
      break;
    }
    case MP_INT8:
      if( iStart+2 <= n )
        sqlite3_result_int64(ctx, (i64)(signed char)a[iStart+1]);
      break;
    case MP_INT16:
      if( iStart+3 <= n )
        sqlite3_result_int64(ctx, (i64)(short)(int)mpRead16(a+iStart+1));
      break;
    case MP_INT32:
      if( iStart+5 <= n )
        sqlite3_result_int64(ctx, (i64)(int)mpRead32(a+iStart+1));
      break;
    case MP_INT64:
      if( iStart+9 <= n )
        sqlite3_result_int64(ctx, (i64)mpRead64(a+iStart+1));
      break;
    case MP_FLOAT32: {
      if( iStart+5 <= n ){
        u32 bits = mpRead32(a+iStart+1);
        float f;
        memcpy(&f, &bits, 4);
        sqlite3_result_double(ctx, (double)f);
      }
      break;
    }
    case MP_FLOAT64: {
      if( iStart+9 <= n ){
        u64 bits = mpRead64(a+iStart+1);
        double d;
        memcpy(&d, &bits, 8);
        sqlite3_result_double(ctx, d);
      }
      break;
    }
    default: {
      /* str → TEXT */
      u32 sLen = 0, sOff = 0;
      if( b >= 0xa0 && b <= 0xbf ){
        sLen = b & 0x1f; sOff = iStart+1;
      } else if( b == MP_STR8  && iStart+2 <= n ){
        sLen = a[iStart+1]; sOff = iStart+2;
      } else if( b == MP_STR16 && iStart+3 <= n ){
        sLen = mpRead16(a+iStart+1); sOff = iStart+3;
      } else if( b == MP_STR32 && iStart+5 <= n ){
        sLen = mpRead32(a+iStart+1); sOff = iStart+5;
      }
      if( sOff ){
        if( sLen > n-sOff ) sLen = n-sOff;
        sqlite3_result_text(ctx, (const char*)(a+sOff), (int)sLen,
                            SQLITE_TRANSIENT);
        return;
      }
      /* bin / array / map / ext → BLOB */
      sqlite3_result_blob(ctx, a+iStart, (int)(iEnd-iStart), SQLITE_TRANSIENT);
      break;
    }
  }
}

/*
** ============================================================
** SQL function implementations
** ============================================================
*/

/*
** -------------------------------------------------------
** Phase 4: Mutation (copy-on-write editing)
** -------------------------------------------------------
*/
#define MP_EDIT_SET       0  /* create or overwrite */
#define MP_EDIT_INSERT    1  /* create only if absent */
#define MP_EDIT_REPLACE   2  /* overwrite only if present */
#define MP_EDIT_REMOVE    3  /* remove element */
#define MP_EDIT_ARRAY_INS 4  /* insert before array index */

/* Forward declaration — mutually recursive with mpEditMap/mpEditArray */
static int mpEditStep(MpBuf*, const u8*, u32, u32,
                      const char*, int, const u8*, u32, int, int*);

/*
** Rebuild the map at iCur, applying the edit for string key zKey/nKey.
** pi: parse position AFTER the key step (remaining sub-path).
** out: output buffer to write rebuilt map into.
*/
static int mpEditMap(
  MpBuf *out,
  const u8 *a, u32 n, u32 iCur,
  const char *zKey, int nKey,
  const char *zPath, int pi,
  const u8 *newBin, u32 nNew,
  int mode
){
  u8 b;
  u32 count, dataOff, j, newCount;
  int foundKey = 0, rc = SQLITE_OK;
  MpBuf tmp;

  if( iCur >= n ) return SQLITE_ERROR;
  b = a[iCur];
  if( b>=0x80 && b<=0x8f ){ count=b&0x0f; dataOff=iCur+1; }
  else if( b==MP_MAP16 ){
    if(iCur+3>n) return SQLITE_ERROR;
    count=mpRead16(a+iCur+1); dataOff=iCur+3;
  } else if( b==MP_MAP32 ){
    if(iCur+5>n) return SQLITE_ERROR;
    count=mpRead32(a+iCur+1); dataOff=iCur+5;
  } else {
    /* Not a map — copy verbatim for non-creating ops, error for creating */
    if( mode==MP_EDIT_REPLACE || mode==MP_EDIT_REMOVE ){
      u32 iEnd = mpSkipOne(a,n,iCur);
      if(iEnd) mpBufAppend(out, a+iCur, iEnd-iCur);
      return SQLITE_OK;
    }
    return SQLITE_ERROR;
  }

  newCount = count;
  mpBufInit(&tmp, out->pCtx);
  u32 cur2 = dataOff;

  for( j=0; j<count; j++ ){
    if( cur2>=n ){ mpBufReset(&tmp); return SQLITE_ERROR; }
    /* Extract key string info without copying */
    u8 kb = a[cur2];
    const char *kStr=0; u32 kLen=0;
    if(kb>=0xa0&&kb<=0xbf)        { kLen=kb&0x1f;           kStr=(const char*)(a+cur2+1); }
    else if(kb==MP_STR8 &&cur2+2<=n){ kLen=a[cur2+1];         kStr=(const char*)(a+cur2+2); }
    else if(kb==MP_STR16&&cur2+3<=n){ kLen=mpRead16(a+cur2+1); kStr=(const char*)(a+cur2+3); }
    else if(kb==MP_STR32&&cur2+5<=n){ kLen=mpRead32(a+cur2+1); kStr=(const char*)(a+cur2+5); }

    u32 valOff  = mpSkipOne(a,n,cur2);
    if(!valOff){ mpBufReset(&tmp); return SQLITE_ERROR; }
    u32 pairEnd = mpSkipOne(a,n,valOff);
    if(!pairEnd){ mpBufReset(&tmp); return SQLITE_ERROR; }

    int isMatch = (kStr && (int)kLen==nKey &&
                   memcmp(kStr, zKey, (size_t)nKey)==0);

    if( isMatch ){
      foundKey = 1;
      if( mode==MP_EDIT_INSERT ){
        /* Key exists — INSERT is a no-op, copy verbatim */
        mpBufAppend(&tmp, a+cur2, pairEnd-cur2);
      } else {
        /* Write modified value into sub-buffer so we can check skip */
        MpBuf vbuf; int skip=0;
        mpBufInit(&vbuf, out->pCtx);
        rc = mpEditStep(&vbuf, a,n, valOff, zPath, pi, newBin, nNew, mode, &skip);
        if( rc!=SQLITE_OK ){ mpBufReset(&vbuf); mpBufReset(&tmp); return rc; }
        if( skip ){
          newCount--;             /* REMOVE: drop key + value entirely */
        } else {
          mpBufAppend(&tmp, a+cur2, valOff-cur2); /* key */
          mpBufAppend(&tmp, vbuf.aBuf, vbuf.nUsed); /* new/modified value */
        }
        mpBufReset(&vbuf);
      }
    } else {
      mpBufAppend(&tmp, a+cur2, pairEnd-cur2); /* copy pair verbatim */
    }
    cur2 = pairEnd;
  }

  if( !foundKey ){
    if( mode==MP_EDIT_SET || mode==MP_EDIT_INSERT ){
      /* Only create direct keys; deep creation not supported */
      int pi2=pi; const char *zk2; int nk2; i64 idx2;
      if( mpPathStep(zPath, &pi2, &zk2, &nk2, &idx2) != 0 ){
        /* Remaining sub-path — can't auto-create intermediate nodes */
        mpBufReset(&tmp);
        u32 iEnd=mpSkipOne(a,n,iCur);
        if(iEnd) mpBufAppend(out, a+iCur, iEnd-iCur);
        return SQLITE_OK;
      }
      /* Append new key/value pair */
      u32 kn=(u32)nKey;
      if(kn<=31)       { mpBufAppend1(&tmp,(u8)(MP_FIXSTR_MASK|kn)); }
      else if(kn<=0xff){ u8 h[2]={MP_STR8,(u8)kn}; mpBufAppend(&tmp,h,2); }
      else             { u8 h[3]; h[0]=MP_STR16; mpWrite16(h+1,(u16)kn); mpBufAppend(&tmp,h,3); }
      mpBufAppend(&tmp, (const u8*)zKey, kn);
      mpBufAppend(&tmp, newBin, nNew);
      newCount++;
    } else {
      /* REPLACE/REMOVE on non-existent key — copy map unchanged */
      mpBufReset(&tmp);
      u32 iEnd=mpSkipOne(a,n,iCur);
      if(iEnd) mpBufAppend(out, a+iCur, iEnd-iCur);
      return SQLITE_OK;
    }
  }

  mpEncodeMapHeader(out, newCount);
  mpBufAppend(out, tmp.aBuf, tmp.nUsed);
  mpBufReset(&tmp);
  return SQLITE_OK;
}

/*
** Rebuild the array at iCur, applying the edit at integer index stepIdx.
*/
static int mpEditArray(
  MpBuf *out,
  const u8 *a, u32 n, u32 iCur,
  i64 stepIdx,
  const char *zPath, int pi,
  const u8 *newBin, u32 nNew,
  int mode
){
  u8 b;
  u32 count, dataOff, j, newCount;
  int foundIt=0, rc=SQLITE_OK;
  MpBuf tmp;

  if( iCur>=n ) return SQLITE_ERROR;
  b = a[iCur];
  if(b>=0x90&&b<=0x9f)       { count=b&0x0f; dataOff=iCur+1; }
  else if(b==MP_ARRAY16){
    if(iCur+3>n) return SQLITE_ERROR;
    count=mpRead16(a+iCur+1); dataOff=iCur+3;
  } else if(b==MP_ARRAY32){
    if(iCur+5>n) return SQLITE_ERROR;
    count=mpRead32(a+iCur+1); dataOff=iCur+5;
  } else {
    if(mode==MP_EDIT_REPLACE || mode==MP_EDIT_REMOVE){
      u32 iEnd=mpSkipOne(a,n,iCur);
      if(iEnd) mpBufAppend(out, a+iCur, iEnd-iCur);
      return SQLITE_OK;
    }
    return SQLITE_ERROR;
  }

  newCount = count;
  mpBufInit(&tmp, out->pCtx);
  u32 cur2 = dataOff;

  for( j=0; j<count; j++ ){
    u32 eEnd = mpSkipOne(a,n,cur2);
    if(!eEnd){ mpBufReset(&tmp); return SQLITE_ERROR; }

    if( (i64)j==stepIdx ){
      foundIt = 1;
      if( mode==MP_EDIT_ARRAY_INS ){
        /* Insert newBin BEFORE element j */
        mpBufAppend(&tmp, newBin, nNew);
        mpBufAppend(&tmp, a+cur2, eEnd-cur2);
        newCount++;
      } else if( mode==MP_EDIT_INSERT ){
        mpBufAppend(&tmp, a+cur2, eEnd-cur2); /* element exists, no-op */
      } else {
        MpBuf ebuf; int skip=0;
        mpBufInit(&ebuf, out->pCtx);
        rc = mpEditStep(&ebuf, a,n, cur2, zPath, pi, newBin, nNew, mode, &skip);
        if(rc!=SQLITE_OK){ mpBufReset(&ebuf); mpBufReset(&tmp); return rc; }
        if(skip){
          newCount--;
        } else {
          mpBufAppend(&tmp, ebuf.aBuf, ebuf.nUsed);
        }
        mpBufReset(&ebuf);
      }
    } else {
      mpBufAppend(&tmp, a+cur2, eEnd-cur2);
    }
    cur2 = eEnd;
  }

  if( !foundIt ){
    if( mode==MP_EDIT_ARRAY_INS ){
      /* Insert at/beyond end → append */
      mpBufAppend(&tmp, newBin, nNew);
      newCount++;
    } else if( (mode==MP_EDIT_SET||mode==MP_EDIT_INSERT)
               && (u64)stepIdx==(u64)count ){
      mpBufAppend(&tmp, newBin, nNew);
      newCount++;
    } else if( mode==MP_EDIT_REPLACE || mode==MP_EDIT_REMOVE ){
      /* Index not found — copy unchanged */
      mpBufReset(&tmp);
      u32 iEnd=mpSkipOne(a,n,iCur);
      if(iEnd) mpBufAppend(out, a+iCur, iEnd-iCur);
      return SQLITE_OK;
    } else {
      mpBufReset(&tmp);
      return SQLITE_NOTFOUND;
    }
  }

  mpEncodeArrayHeader(out, newCount);
  mpBufAppend(out, tmp.aBuf, tmp.nUsed);
  mpBufReset(&tmp);
  return SQLITE_OK;
}

/*
** Core recursive edit function.
** iCur: offset of current element in (a,n).
** zPath/pi: full path string and current parse position.
** pSkip: set to 1 if this element was removed (parent should drop it).
*/
static int mpEditStep(
  MpBuf *out,
  const u8 *a, u32 n, u32 iCur,
  const char *zPath, int pi,
  const u8 *newBin, u32 nNew,
  int mode,
  int *pSkip
){
  const char *zKey=0; int nKey=0; i64 stepIdx=0;
  int step = mpPathStep(zPath, &pi, &zKey, &nKey, &stepIdx);
  if(pSkip) *pSkip=0;

  if( step==0 ){
    if( mode==MP_EDIT_REMOVE ){
      if(pSkip) *pSkip=1;   /* signal parent to drop this element */
      return SQLITE_OK;
    }
    if( mode==MP_EDIT_ARRAY_INS ) return SQLITE_ERROR;
    if( mode==MP_EDIT_INSERT ){
      /* Element already exists — INSERT is a no-op, copy original */
      u32 iEnd=mpSkipOne(a,n,iCur);
      if(iEnd) mpBufAppend(out, a+iCur, iEnd-iCur);
      return SQLITE_OK;
    }
    mpBufAppend(out, newBin, nNew);  /* SET or REPLACE */
    return SQLITE_OK;
  }
  if( step<0 ) return SQLITE_ERROR;

  if( step=='k' ){
    return mpEditMap(out, a,n,iCur, zKey,nKey, zPath,pi, newBin,nNew, mode);
  } else {
    return mpEditArray(out, a,n,iCur, stepIdx, zPath,pi, newBin,nNew, mode);
  }
}

/*
** Apply a single edit. zPath must start with '$'.
*/
static int mpApplyEdit(
  MpBuf *out,
  const u8 *a, u32 n,
  const char *zPath,
  const u8 *newBin, u32 nNew,
  int mode
){
  if(!zPath || zPath[0]!='$') return SQLITE_ERROR;
  return mpEditStep(out, a,n, 0, zPath, 1, newBin, nNew, mode, 0);
}

/* ---- Common driver for set/insert/replace/array_insert ---- */
static void msgpackEditFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv, int mode
){
  const u8 *a; u32 n;
  int i;
  if( argc<3 || (argc%2)==0 ){
    sqlite3_result_error(ctx,
      "msgpack edit functions require mp, path, val [, path, val ...]", -1);
    return;
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_null(ctx); return;
  }
  a=(const u8*)sqlite3_value_blob(argv[0]);
  n=(u32)sqlite3_value_bytes(argv[0]);

  /* Chain edits; cur starts as a copy of the input blob */
  u8 *cur=(u8*)sqlite3_malloc(n>0?n:1);
  if(!cur){ sqlite3_result_error_nomem(ctx); return; }
  if(n) memcpy(cur,a,n);
  u32 nCur=n;

  for( i=1; i<argc-1; i+=2 ){
    const char *zPath=(const char*)sqlite3_value_text(argv[i]);

    /* Encode new value */
    MpBuf valBuf; mpBufInit(&valBuf,ctx);
    mpEncodeSqlValue(&valBuf, argv[i+1]);
    u32 nNew; u8 *newBin=mpBufFinish(&valBuf,&nNew);
    if(!newBin){ sqlite3_free(cur); return; }

    MpBuf outBuf; mpBufInit(&outBuf,ctx);
    int rc=mpApplyEdit(&outBuf, cur,nCur, zPath, newBin,nNew, mode);
    sqlite3_free(newBin);

    u32 nOut; u8 *outData=mpBufFinish(&outBuf,&nOut);
    sqlite3_free(cur);
    if(!outData) return;
    if(rc!=SQLITE_OK){
      /* On path-miss for REPLACE/REMOVE, outData is the unchanged blob */
      cur=outData; nCur=nOut;
    } else {
      cur=outData; nCur=nOut;
    }
  }
  sqlite3_result_blob(ctx, cur, (int)nCur, sqlite3_free);
}

/* ---- Remove: takes mp + one or more paths (no values) ---- */
static void msgpackRemoveFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  const u8 *a; u32 n;
  int i;
  if(argc<2){
    sqlite3_result_error(ctx,"msgpack_remove requires mp and at least one path",-1);
    return;
  }
  if(sqlite3_value_type(argv[0])!=SQLITE_BLOB){
    sqlite3_result_null(ctx); return;
  }
  a=(const u8*)sqlite3_value_blob(argv[0]);
  n=(u32)sqlite3_value_bytes(argv[0]);

  u8 *cur=(u8*)sqlite3_malloc(n>0?n:1);
  if(!cur){ sqlite3_result_error_nomem(ctx); return; }
  if(n) memcpy(cur,a,n);
  u32 nCur=n;

  for( i=1; i<argc; i++ ){
    const char *zPath=(const char*)sqlite3_value_text(argv[i]);
    MpBuf outBuf; mpBufInit(&outBuf,ctx);
    mpApplyEdit(&outBuf, cur,nCur, zPath, 0,0, MP_EDIT_REMOVE);
    u32 nOut; u8 *outData=mpBufFinish(&outBuf,&nOut);
    sqlite3_free(cur);
    if(!outData) return;
    cur=outData; nCur=nOut;
  }
  sqlite3_result_blob(ctx, cur, (int)nCur, sqlite3_free);
}

/* ---- RFC-7386 merge-patch ---- */
static int mpMergePatch(
  MpBuf *out,
  const u8 *a, u32 n,  u32 ia,   /* target element */
  const u8 *p, u32 np, u32 ip,   /* patch element */
  int depth
);

static int mpMergePatch(
  MpBuf *out,
  const u8 *a, u32 n,  u32 ia,
  const u8 *p, u32 np, u32 ip,
  int depth
){
  if(ip>=np) return SQLITE_ERROR;
  if(depth>MP_MAX_DEPTH) return SQLITE_ERROR;
  u8 pb = p[ip];

  /* If patch is nil → result is nil */
  if(pb==MP_NIL){ mpBufAppend1(out,MP_NIL); return SQLITE_OK; }

  /* If patch is not a map → return patch as-is */
  int pIsMap = (pb>=0x80&&pb<=0x8f)||pb==MP_MAP16||pb==MP_MAP32;
  if(!pIsMap){
    u32 pEnd=mpSkipOne(p,np,ip);
    if(pEnd) mpBufAppend(out, p+ip, pEnd-ip);
    return SQLITE_OK;
  }

  /* Patch is a map; target may or may not be a map */
  u8 ab = (ia<n)?a[ia]:0;
  int aIsMap = (ab>=0x80&&ab<=0x8f)||ab==MP_MAP16||ab==MP_MAP32;

  u32 pCount,pDataOff;
  if(pb>=0x80&&pb<=0x8f)     { pCount=pb&0x0f;           pDataOff=ip+1; }
  else if(pb==MP_MAP16){
    if(ip+3>np) return SQLITE_ERROR;
    pCount=mpRead16(p+ip+1);  pDataOff=ip+3;
  } else {
    if(ip+5>np) return SQLITE_ERROR;
    pCount=mpRead32(p+ip+1);  pDataOff=ip+5;
  }

  u32 aCount=0, aDataOff=0;
  if(aIsMap){
    if(ab>=0x80&&ab<=0x8f)    { aCount=ab&0x0f;           aDataOff=ia+1; }
    else if(ab==MP_MAP16){
      if(ia+3>n){ aIsMap=0; }
      else { aCount=mpRead16(a+ia+1);  aDataOff=ia+3; }
    } else {
      if(ia+5>n){ aIsMap=0; }
      else { aCount=mpRead32(a+ia+1);  aDataOff=ia+5; }
    }
  }

  MpBuf tmp; mpBufInit(&tmp,out->pCtx);
  u32 newCount=0;

  /* Phase 1: iterate target pairs. For each:
  **   - If the key is nil-valued in patch → drop it
  **   - If the key is in patch with non-nil value → merge recursively
  **   - Otherwise → copy verbatim */
  u32 ac=aDataOff;
  for(u32 j=0; j<aCount; j++){
    if(ac>=n){ mpBufReset(&tmp); return SQLITE_ERROR; }
    u8 kb=a[ac];
    const char *kStr=0; u32 kLen=0;
    if(kb>=0xa0&&kb<=0xbf)        {kLen=kb&0x1f;           kStr=(const char*)(a+ac+1);}
    else if(kb==MP_STR8 &&ac+2<=n){kLen=a[ac+1];           kStr=(const char*)(a+ac+2);}
    else if(kb==MP_STR16&&ac+3<=n){kLen=mpRead16(a+ac+1);  kStr=(const char*)(a+ac+3);}
    else if(kb==MP_STR32&&ac+5<=n){kLen=mpRead32(a+ac+1);  kStr=(const char*)(a+ac+5);}

    u32 aValOff=mpSkipOne(a,n,ac);
    if(!aValOff){mpBufReset(&tmp);return SQLITE_ERROR;}
    u32 aPairEnd=mpSkipOne(a,n,aValOff);
    if(!aPairEnd){mpBufReset(&tmp);return SQLITE_ERROR;}

    /* Find this key in patch */
    int foundInPatch=0, patchIsNil=0;
    u32 pMatchVal=0;
    u32 pc2=pDataOff;
    for(u32 k=0; k<pCount; k++){
      if(pc2>=np) break;
      u8 pkb=p[pc2];
      const char *pkStr=0; u32 pkLen=0;
      if(pkb>=0xa0&&pkb<=0xbf)         {pkLen=pkb&0x1f;           pkStr=(const char*)(p+pc2+1);}
      else if(pkb==MP_STR8 &&pc2+2<=np){pkLen=p[pc2+1];           pkStr=(const char*)(p+pc2+2);}
      else if(pkb==MP_STR16&&pc2+3<=np){pkLen=mpRead16(p+pc2+1);  pkStr=(const char*)(p+pc2+3);}
      else if(pkb==MP_STR32&&pc2+5<=np){pkLen=mpRead32(p+pc2+1);  pkStr=(const char*)(p+pc2+5);}
      u32 pvOff=mpSkipOne(p,np,pc2); if(!pvOff) break;
      u32 ppEnd=mpSkipOne(p,np,pvOff); if(!ppEnd) break;
      if(pkStr && kStr && (int)pkLen==(int)kLen &&
         memcmp(pkStr,kStr,kLen)==0){
        foundInPatch=1; pMatchVal=pvOff;
        patchIsNil=(pvOff<np && p[pvOff]==MP_NIL);
        break;
      }
      pc2=ppEnd;
    }

    if(foundInPatch && patchIsNil){
      /* Drop this pair */
    } else if(foundInPatch){
      MpBuf mb; mpBufInit(&mb,out->pCtx);
      int mrc=mpMergePatch(&mb, a,n,aValOff, p,np,pMatchVal, depth+1);
      if(mrc==SQLITE_OK){
        mpBufAppend(&tmp, a+ac, aValOff-ac); /* key */
        mpBufAppend(&tmp, mb.aBuf, mb.nUsed); /* merged val */
        newCount++;
      }
      mpBufReset(&mb);
    } else {
      mpBufAppend(&tmp, a+ac, aPairEnd-ac);
      newCount++;
    }
    ac=aPairEnd;
  }

  /* Phase 2: add patch pairs not already in target */
  u32 pc=pDataOff;
  for(u32 k=0; k<pCount; k++){
    if(pc>=np) break;
    u8 pkb=p[pc];
    const char *pkStr=0; u32 pkLen=0;
    if(pkb>=0xa0&&pkb<=0xbf)         {pkLen=pkb&0x1f;           pkStr=(const char*)(p+pc+1);}
    else if(pkb==MP_STR8 &&pc+2<=np) {pkLen=p[pc+1];            pkStr=(const char*)(p+pc+2);}
    else if(pkb==MP_STR16&&pc+3<=np) {pkLen=mpRead16(p+pc+1);   pkStr=(const char*)(p+pc+3);}
    else if(pkb==MP_STR32&&pc+5<=np) {pkLen=mpRead32(p+pc+1);   pkStr=(const char*)(p+pc+5);}
    u32 pvOff=mpSkipOne(p,np,pc); if(!pvOff) break;
    u32 ppEnd=mpSkipOne(p,np,pvOff); if(!ppEnd) break;

    /* Is this key already in target? */
    int inTarget=0;
    if(aIsMap){
      u32 ac2=aDataOff;
      for(u32 j=0; j<aCount; j++){
        if(ac2>=n) break;
        u8 tkb=a[ac2];
        const char *tkStr=0; u32 tkLen=0;
        if(tkb>=0xa0&&tkb<=0xbf)         {tkLen=tkb&0x1f;           tkStr=(const char*)(a+ac2+1);}
        else if(tkb==MP_STR8 &&ac2+2<=n) {tkLen=a[ac2+1];           tkStr=(const char*)(a+ac2+2);}
        else if(tkb==MP_STR16&&ac2+3<=n) {tkLen=mpRead16(a+ac2+1);  tkStr=(const char*)(a+ac2+3);}
        else if(tkb==MP_STR32&&ac2+5<=n) {tkLen=mpRead32(a+ac2+1);  tkStr=(const char*)(a+ac2+5);}
        u32 tvOff=mpSkipOne(a,n,ac2); if(!tvOff) break;
        u32 tpEnd=mpSkipOne(a,n,tvOff); if(!tpEnd) break;
        if(tkStr&&pkStr&&(int)tkLen==(int)pkLen&&memcmp(tkStr,pkStr,tkLen)==0){
          inTarget=1; break;
        }
        ac2=tpEnd;
      }
    }

    /* Add if not in target and patch value is not nil */
    if(!inTarget && pvOff<np && p[pvOff]!=MP_NIL){
      mpBufAppend(&tmp, p+pc, ppEnd-pc);
      newCount++;
    }
    pc=ppEnd;
  }

  mpEncodeMapHeader(out, newCount);
  mpBufAppend(out, tmp.aBuf, tmp.nUsed);
  mpBufReset(&tmp);
  return SQLITE_OK;
}

/* ---- SQL wrapper functions ---- */
static void msgpackSetFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){ msgpackEditFunc(ctx, argc, argv, MP_EDIT_SET); }

static void msgpackInsertFunc2(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){ msgpackEditFunc(ctx, argc, argv, MP_EDIT_INSERT); }

static void msgpackReplaceFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){ msgpackEditFunc(ctx, argc, argv, MP_EDIT_REPLACE); }

static void msgpackArrayInsertFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){ msgpackEditFunc(ctx, argc, argv, MP_EDIT_ARRAY_INS); }

static void msgpackPatchFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  const u8 *a, *p; u32 n, np;
  (void)argc;
  if(sqlite3_value_type(argv[0])!=SQLITE_BLOB ||
     sqlite3_value_type(argv[1])!=SQLITE_BLOB){
    sqlite3_result_null(ctx); return;
  }
  a=(const u8*)sqlite3_value_blob(argv[0]); n=(u32)sqlite3_value_bytes(argv[0]);
  p=(const u8*)sqlite3_value_blob(argv[1]); np=(u32)sqlite3_value_bytes(argv[1]);
  MpBuf out; mpBufInit(&out,ctx);
  int rc=mpMergePatch(&out, a,n,0, p,np,0, 0);
  if(rc==SQLITE_OK){
    u32 nOut; u8 *res=mpBufFinish(&out,&nOut);
    if(res) sqlite3_result_blob(ctx,res,(int)nOut,sqlite3_free);
  } else {
    mpBufReset(&out);
    sqlite3_result_error(ctx,"msgpack_patch error",-1);
  }
}



/*
** msgpack(value) → BLOB
** Validate an existing msgpack BLOB and return it.  NULL → NULL.
*/
static void msgpackFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *a;
  u32 n;
  (void)argc;
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    sqlite3_result_null(ctx); return;
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_error(ctx, "msgpack() requires a BLOB argument", -1); return;
  }
  a = (const u8*)sqlite3_value_blob(argv[0]);
  n = (u32)sqlite3_value_bytes(argv[0]);
  if( !mpIsValid(a, n) ){
    sqlite3_result_error(ctx, "invalid msgpack data", -1); return;
  }
  sqlite3_result_blob(ctx, a, (int)n, SQLITE_TRANSIENT);
}

/*
** msgpack_array(v1, v2, ...) → BLOB
** Build a msgpack array from the given SQL values.
*/
static void msgpackArrayFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  MpBuf buf;
  u8 *result;
  u32 nResult;
  int i;
  mpBufInit(&buf, ctx);
  mpEncodeArrayHeader(&buf, (u32)argc);
  for( i=0; i<argc; i++ ) mpEncodeSqlValue(&buf, argv[i]);
  result = mpBufFinish(&buf, &nResult);
  if( result ) sqlite3_result_blob(ctx, result, (int)nResult, sqlite3_free);
}

/*
** msgpack_object(k1, v1, k2, v2, ...) → BLOB
** Build a msgpack map from the given key/value pairs.
** Error if odd number of arguments.
*/
static void msgpackObjectFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  MpBuf buf;
  u8 *result;
  u32 nResult;
  int i;
  if( argc % 2 != 0 ){
    sqlite3_result_error(ctx,
      "msgpack_object() requires an even number of arguments", -1);
    return;
  }
  mpBufInit(&buf, ctx);
  mpEncodeMapHeader(&buf, (u32)(argc/2));
  for( i=0; i<argc; i++ ) mpEncodeSqlValue(&buf, argv[i]);
  result = mpBufFinish(&buf, &nResult);
  if( result ) sqlite3_result_blob(ctx, result, (int)nResult, sqlite3_free);
}

/*
** -------------------------------------------------------
** Phase 3: Extraction functions
** -------------------------------------------------------
*/

/*
** msgpack_extract(mp, path, ...) → value
** Single path: returns SQL-typed value for scalars, BLOB for containers.
** Multiple paths: returns a msgpack array of results (one per path).
*/
static void msgpackExtractFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *a;
  u32 n;
  if( argc < 2 ){
    sqlite3_result_error(ctx,
      "msgpack_extract requires at least 2 arguments", -1);
    return;
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_null(ctx); return;
  }
  a = (const u8*)sqlite3_value_blob(argv[0]);
  n = (u32)sqlite3_value_bytes(argv[0]);

  if( argc == 2 ){
    const char *zPath = (const char*)sqlite3_value_text(argv[1]);
    u32 iStart=0, iEnd=0;
    if( mpLookup(a,n,0,zPath,&iStart,&iEnd)==SQLITE_OK ){
      mpReturnElement(ctx, a, n, iStart, iEnd);
    } else {
      sqlite3_result_null(ctx);
    }
  } else {
    /* Multiple paths → return msgpack array */
    MpBuf buf;
    u8 *result;
    u32 nResult;
    int i;
    mpBufInit(&buf, ctx);
    mpEncodeArrayHeader(&buf, (u32)(argc-1));
    for( i=1; i<argc; i++ ){
      const char *zPath = (const char*)sqlite3_value_text(argv[i]);
      u32 iStart=0, iEnd=0;
      if( mpLookup(a,n,0,zPath,&iStart,&iEnd)==SQLITE_OK ){
        mpBufAppend(&buf, a+iStart, iEnd-iStart);
      } else {
        mpBufAppend1(&buf, MP_NIL);
      }
    }
    result = mpBufFinish(&buf, &nResult);
    if( result ) sqlite3_result_blob(ctx, result, (int)nResult, sqlite3_free);
  }
}

/*
** msgpack_type(mp [, path]) → TEXT
** Returns the type name of the element at path (or the root element).
** One of: null, true, false, integer, real, text, blob, array, map, ext
*/
static void msgpackTypeFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *a;
  u32 n, iStart=0;
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_null(ctx); return;
  }
  a = (const u8*)sqlite3_value_blob(argv[0]);
  n = (u32)sqlite3_value_bytes(argv[0]);
  if( n==0 ){ sqlite3_result_null(ctx); return; }

  if( argc >= 2 ){
    u32 iEnd=0;
    const char *zPath = (const char*)sqlite3_value_text(argv[1]);
    if( mpLookup(a,n,0,zPath,&iStart,&iEnd)!=SQLITE_OK ){
      sqlite3_result_null(ctx); return;
    }
  }
  sqlite3_result_text(ctx, mpGetTypeStr(a,n,iStart), -1, SQLITE_STATIC);
}

/*
** msgpack_array_length(mp [, path]) → INT
** Element count for an array, pair count for a map.
** Returns NULL if the target is not a container.
*/
static void msgpackArrayLengthFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *a;
  u32 n, iStart=0;
  i64 count;
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_null(ctx); return;
  }
  a = (const u8*)sqlite3_value_blob(argv[0]);
  n = (u32)sqlite3_value_bytes(argv[0]);
  if( n==0 ){ sqlite3_result_null(ctx); return; }

  if( argc >= 2 ){
    u32 iEnd=0;
    const char *zPath = (const char*)sqlite3_value_text(argv[1]);
    if( mpLookup(a,n,0,zPath,&iStart,&iEnd)!=SQLITE_OK ){
      sqlite3_result_null(ctx); return;
    }
  }
  count = mpGetContainerCount(a, n, iStart);
  if( count < 0 ) sqlite3_result_null(ctx);
  else            sqlite3_result_int64(ctx, count);
}

/*
** msgpack_error_position(mp) → INT
** Returns 1-based byte offset of first encoding error, or 0 if valid.
*/
static void msgpackErrorPositionFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *a;
  u32 n, end;
  (void)argc;
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_null(ctx); return;
  }
  a = (const u8*)sqlite3_value_blob(argv[0]);
  n = (u32)sqlite3_value_bytes(argv[0]);
  if( n==0 ){ sqlite3_result_int(ctx, 1); return; }
  end = mpSkipOne(a, n, 0);
  if( end == n ){
    sqlite3_result_int(ctx, 0);   /* valid */
  } else if( end == 0 ){
    sqlite3_result_int(ctx, 1);   /* bad at byte 1 */
  } else {
    sqlite3_result_int64(ctx, (i64)(end+1)); /* trailing garbage at end+1 */
  }
}

/*
** -------------------------------------------------------
** Phase 1: Validation + quote
** -------------------------------------------------------
*/

/*
** msgpack_valid(mp [, flags])
**   Returns 1 if mp is a well-formed msgpack BLOB, 0 otherwise.
**   The optional flags argument is reserved for future strictness levels.
*/
static void msgpackValidFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *a;
  u32 n;
  (void)argc;
  if( sqlite3_value_type(argv[0]) == SQLITE_NULL ){
    sqlite3_result_null(ctx);
    return;
  }
  if( sqlite3_value_type(argv[0]) != SQLITE_BLOB ){
    sqlite3_result_int(ctx, 0);
    return;
  }
  a = (const u8*)sqlite3_value_blob(argv[0]);
  n = (u32)sqlite3_value_bytes(argv[0]);
  sqlite3_result_int(ctx, mpIsValid(a, n));
}

/*
** msgpack_quote(value)
**   Encodes a single SQL value as msgpack BLOB.
**   NULL → nil, INTEGER → int, REAL → float64, TEXT → str, BLOB → bin
*/
static void msgpackQuoteFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  MpBuf buf;
  u8 *result;
  u32 nResult;
  (void)argc;
  mpBufInit(&buf, ctx);
  mpEncodeSqlValue(&buf, argv[0]);
  result = mpBufFinish(&buf, &nResult);
  if( result ){
    sqlite3_result_blob(ctx, result, (int)nResult, sqlite3_free);
  }
}

/*
** -------------------------------------------------------
** Phase 5: Conversion — msgpack ↔ JSON
** -------------------------------------------------------
*/

/* Write a JSON-escaped string (with surrounding quotes) to out */
static void mpJsonEscapeStr(MpBuf *out, const u8 *s, u32 len){
  u32 j;
  mpBufAppend1(out, '"');
  for( j=0; j<len; j++ ){
    u8 c = s[j];
    if( c=='"' ){
      u8 b[2]={'\\','"'}; mpBufAppend(out,b,2);
    } else if( c=='\\' ){
      u8 b[2]={'\\','\\'}; mpBufAppend(out,b,2);
    } else if( c=='\n' ){
      u8 b[2]={'\\','n'}; mpBufAppend(out,b,2);
    } else if( c=='\r' ){
      u8 b[2]={'\\','r'}; mpBufAppend(out,b,2);
    } else if( c=='\t' ){
      u8 b[2]={'\\','t'}; mpBufAppend(out,b,2);
    } else if( c < 0x20 ){
      char esc[8]; sqlite3_snprintf(8,esc,"\\u%04x",(int)c);
      mpBufAppend(out,(const u8*)esc,6);
    } else {
      mpBufAppend1(out,c);
    }
  }
  mpBufAppend1(out, '"');
}

/* Emit N spaces for pretty-printing indentation */
static void mpJsonNewline(MpBuf *out, int depth, int indentW){
  int i;
  mpBufAppend1(out, '\n');
  for( i=0; i<depth*indentW; i++ ) mpBufAppend1(out,' ');
}

/* Recursively convert msgpack element at offset i to JSON text in out */
static void mpToJsonAt(
  MpBuf *out, const u8 *a, u32 n, u32 i,
  int pretty, int depth, int indentW
){
  u8 b;
  char s[32];

  if( i>=n ){ mpBufAppend(out,(const u8*)"null",4); return; }
  if( depth>MP_MAX_DEPTH ){ mpBufAppend(out,(const u8*)"null",4); return; }
  b = a[i];

  /* nil */
  if(b==MP_NIL)  { mpBufAppend(out,(const u8*)"null",4);  return; }
  if(b==MP_FALSE){ mpBufAppend(out,(const u8*)"false",5); return; }
  if(b==MP_TRUE) { mpBufAppend(out,(const u8*)"true",4);  return; }
  /* positive fixint */
  if(b<=0x7f){ sqlite3_snprintf(32,s,"%d",(int)b); mpBufAppend(out,(const u8*)s,(u32)strlen(s)); return; }
  /* negative fixint */
  if(b>=0xe0){ sqlite3_snprintf(32,s,"%d",(int)(signed char)b); mpBufAppend(out,(const u8*)s,(u32)strlen(s)); return; }

  switch(b){
    case MP_UINT8:  if(i+2>n) break; sqlite3_snprintf(32,s,"%u",(unsigned)a[i+1]);                    goto numout;
    case MP_UINT16: if(i+3>n) break; sqlite3_snprintf(32,s,"%u",(unsigned)mpRead16(a+i+1));           goto numout;
    case MP_UINT32: if(i+5>n) break; sqlite3_snprintf(32,s,"%u",(unsigned)mpRead32(a+i+1));           goto numout;
    case MP_UINT64: if(i+9>n) break; sqlite3_snprintf(32,s,"%llu",(unsigned long long)mpRead64(a+i+1));goto numout;
    case MP_INT8:   if(i+2>n) break; sqlite3_snprintf(32,s,"%d",(int)(signed char)a[i+1]);            goto numout;
    case MP_INT16:  if(i+3>n) break; sqlite3_snprintf(32,s,"%d",(int)(short)mpRead16(a+i+1));         goto numout;
    case MP_INT32:  if(i+5>n) break; sqlite3_snprintf(32,s,"%d",(int)mpRead32(a+i+1));                goto numout;
    case MP_INT64:  if(i+9>n) break; sqlite3_snprintf(32,s,"%lld",(long long)mpRead64(a+i+1));        goto numout;
    numout: mpBufAppend(out,(const u8*)s,(u32)strlen(s)); return;

    case MP_FLOAT32: {
      if(i+5>n) break;
      u32 bits=mpRead32(a+i+1); float f; memcpy(&f,&bits,4);
      if( !isfinite((double)f) ){ mpBufAppend(out,(const u8*)"null",4); return; }
      sqlite3_snprintf(32,s,"%.7g",(double)f);
      goto fltout;
    }
    case MP_FLOAT64: {
      if(i+9>n) break;
      u64 bits=mpRead64(a+i+1); double d; memcpy(&d,&bits,8);
      if( !isfinite(d) ){ mpBufAppend(out,(const u8*)"null",4); return; }
      sqlite3_snprintf(32,s,"%.17g",d);
      /* ensure JSON-valid number has decimal point */
      if(!strchr(s,'.') && !strchr(s,'e') && !strchr(s,'E'))
        sqlite3_snprintf(32,s,"%.1f",d);
      fltout: mpBufAppend(out,(const u8*)s,(u32)strlen(s)); return;
    }
    default: break;
  }

  /* str */
  { u32 sLen=0, sOff=0;
    if(b>=0xa0&&b<=0xbf)       { sLen=b&0x1f; sOff=i+1; }
    else if(b==MP_STR8 &&i+2<=n){ sLen=a[i+1]; sOff=i+2; }
    else if(b==MP_STR16&&i+3<=n){ sLen=mpRead16(a+i+1); sOff=i+3; }
    else if(b==MP_STR32&&i+5<=n){ sLen=mpRead32(a+i+1); sOff=i+5; }
    if( sOff ){
      if( sLen > n-sOff ) sLen = n-sOff;
      mpJsonEscapeStr(out, a+sOff, sLen);
      return;
    }
  }

  /* bin → hex string */
  { u32 bLen=0, bOff=0;
    if(b==MP_BIN8 &&i+2<=n){ bLen=a[i+1]; bOff=i+2; }
    else if(b==MP_BIN16&&i+3<=n){ bLen=mpRead16(a+i+1); bOff=i+3; }
    else if(b==MP_BIN32&&i+5<=n){ bLen=mpRead32(a+i+1); bOff=i+5; }
    if( bOff ){
      static const char hex[]="0123456789abcdef";
      u32 j;
      if( bLen > n-bOff ) bLen = n-bOff;
      mpBufAppend1(out,'"');
      for(j=0; j<bLen; j++){
        u8 by=a[bOff+j];
        mpBufAppend1(out,(u8)hex[by>>4]);
        mpBufAppend1(out,(u8)hex[by&0xf]);
      }
      mpBufAppend1(out,'"');
      return;
    }
  }

  /* array */
  { int isArr=0; u32 count=0, dataOff=0;
    if(b>=0x90&&b<=0x9f)         { isArr=1; count=b&0x0f; dataOff=i+1; }
    else if(b==MP_ARRAY16&&i+3<=n){ isArr=1; count=mpRead16(a+i+1); dataOff=i+3; }
    else if(b==MP_ARRAY32&&i+5<=n){ isArr=1; count=mpRead32(a+i+1); dataOff=i+5; }
    if( isArr ){
      u32 cur=dataOff, j;
      mpBufAppend1(out,'[');
      for(j=0; j<count; j++){
        if( cur >= n ) break;
        u32 next=mpSkipOne(a,n,cur);
        if(j>0) mpBufAppend1(out,',');
        if(pretty) mpJsonNewline(out,depth+1,indentW);
        mpToJsonAt(out,a,n,cur,pretty,depth+1,indentW);
        cur=next?next:n;
      }
      if(pretty&&count>0) mpJsonNewline(out,depth,indentW);
      mpBufAppend1(out,']');
      return;
    }
  }

  /* map */
  { int isMap=0; u32 count=0, dataOff=0;
    if(b>=0x80&&b<=0x8f)        { isMap=1; count=b&0x0f; dataOff=i+1; }
    else if(b==MP_MAP16&&i+3<=n){ isMap=1; count=mpRead16(a+i+1); dataOff=i+3; }
    else if(b==MP_MAP32&&i+5<=n){ isMap=1; count=mpRead32(a+i+1); dataOff=i+5; }
    if( isMap ){
      u32 cur=dataOff, j;
      mpBufAppend1(out,'{');
      for(j=0; j<count; j++){
        if( cur >= n ) break;
        u32 valOff=mpSkipOne(a,n,cur);
        u32 pairEnd=valOff?mpSkipOne(a,n,valOff):0;
        if(j>0) mpBufAppend1(out,',');
        if(pretty) mpJsonNewline(out,depth+1,indentW);
        mpToJsonAt(out,a,n,cur,pretty,depth+1,indentW);
        mpBufAppend1(out,':');
        if(pretty) mpBufAppend1(out,' ');
        mpToJsonAt(out,a,n,valOff?valOff:n,pretty,depth+1,indentW);
        cur=pairEnd?pairEnd:n;
      }
      if(pretty&&count>0) mpJsonNewline(out,depth,indentW);
      mpBufAppend1(out,'}');
      return;
    }
  }

  /* ext / unknown → null */
  mpBufAppend(out,(const u8*)"null",4);
}

/* ---- JSON parser → msgpack encoder ---- */

typedef struct MpJsonParser {
  const char *z;
  int n, i;
} MpJsonParser;

static void mpJpSkipWS(MpJsonParser *p){
  while(p->i<p->n &&
        (p->z[p->i]==' '||p->z[p->i]=='\t'||
         p->z[p->i]=='\n'||p->z[p->i]=='\r')) p->i++;
}

static int mpJpHex4(const char *z){
  int v=0, j;
  for(j=0;j<4;j++){
    char c=z[j]; int h;
    if(c>='0'&&c<='9') h=c-'0';
    else if(c>='a'&&c<='f') h=c-'a'+10;
    else if(c>='A'&&c<='F') h=c-'A'+10;
    else return -1;
    v=(v<<4)|h;
  }
  return v;
}

static int mpJpCodepointToUtf8(u32 cp, u8 *buf){
  if(cp<0x80)    { buf[0]=(u8)cp; return 1; }
  if(cp<0x800)   { buf[0]=(u8)(0xc0|(cp>>6)); buf[1]=(u8)(0x80|(cp&0x3f)); return 2; }
  if(cp<0x10000) { buf[0]=(u8)(0xe0|(cp>>12)); buf[1]=(u8)(0x80|((cp>>6)&0x3f)); buf[2]=(u8)(0x80|(cp&0x3f)); return 3; }
  buf[0]=(u8)(0xf0|(cp>>18)); buf[1]=(u8)(0x80|((cp>>12)&0x3f));
  buf[2]=(u8)(0x80|((cp>>6)&0x3f)); buf[3]=(u8)(0x80|(cp&0x3f)); return 4;
}

static int mpJpParseValue(MpJsonParser *p, MpBuf *out); /* forward */

static int mpJpParseString(MpJsonParser *p, MpBuf *out){
  MpBuf sb;
  mpBufInit(&sb, out->pCtx);
  p->i++; /* skip '"' */
  while(p->i<p->n){
    unsigned char c=(unsigned char)p->z[p->i];
    if(c=='"'){ p->i++; break; }
    if(c=='\\'){
      p->i++;
      if(p->i>=p->n){ mpBufReset(&sb); return SQLITE_ERROR; }
      char esc=p->z[p->i++];
      switch(esc){
        case '"': mpBufAppend1(&sb,'"'); break;
        case '\\':mpBufAppend1(&sb,'\\'); break;
        case '/': mpBufAppend1(&sb,'/'); break;
        case 'n': mpBufAppend1(&sb,'\n'); break;
        case 'r': mpBufAppend1(&sb,'\r'); break;
        case 't': mpBufAppend1(&sb,'\t'); break;
        case 'b': mpBufAppend1(&sb,'\b'); break;
        case 'f': mpBufAppend1(&sb,'\f'); break;
        case 'u': {
          if(p->i+4>p->n){ mpBufReset(&sb); return SQLITE_ERROR; }
          int cp=mpJpHex4(p->z+p->i); p->i+=4;
          if(cp<0){ mpBufReset(&sb); return SQLITE_ERROR; }
          /* Handle UTF-16 surrogate pairs */
          if(cp>=0xD800&&cp<=0xDBFF&&p->i+6<=p->n&&
             p->z[p->i]=='\\'&&p->z[p->i+1]=='u'){
            int lo=mpJpHex4(p->z+p->i+2);
            if(lo>=0xDC00&&lo<=0xDFFF){
              p->i+=6;
              cp=0x10000+((cp-0xD800)<<10)+(lo-0xDC00);
            }
          }
          u8 utf[4]; int ulen=mpJpCodepointToUtf8((u32)cp,utf);
          mpBufAppend(&sb,utf,(u32)ulen);
          break;
        }
        default: mpBufAppend1(&sb,(u8)esc); break;
      }
    } else {
      mpBufAppend1(&sb,c); p->i++;
    }
  }
  /* Emit as msgpack str */
  u32 sLen=sb.nUsed;
  if(sLen<=31)      { mpBufAppend1(out,(u8)(MP_FIXSTR_MASK|sLen)); }
  else if(sLen<=0xff){ u8 h[2]={MP_STR8,(u8)sLen}; mpBufAppend(out,h,2); }
  else if(sLen<=0xffff){ u8 h[3]; h[0]=MP_STR16; mpWrite16(h+1,(u16)sLen); mpBufAppend(out,h,3); }
  else              { u8 h[5]; h[0]=MP_STR32; mpWrite32(h+1,sLen); mpBufAppend(out,h,5); }
  mpBufAppend(out, sb.aBuf, sb.nUsed);
  mpBufReset(&sb);
  return SQLITE_OK;
}

static int mpJpParseNumber(MpJsonParser *p, MpBuf *out){
  int start=p->i, isFloat=0;
  char buf[64];
  int len;
  if(p->i<p->n&&p->z[p->i]=='-') p->i++;
  while(p->i<p->n&&p->z[p->i]>='0'&&p->z[p->i]<='9') p->i++;
  if(p->i<p->n&&p->z[p->i]=='.'){
    isFloat=1; p->i++;
    while(p->i<p->n&&p->z[p->i]>='0'&&p->z[p->i]<='9') p->i++;
  }
  if(p->i<p->n&&(p->z[p->i]=='e'||p->z[p->i]=='E')){
    isFloat=1; p->i++;
    if(p->i<p->n&&(p->z[p->i]=='+'||p->z[p->i]=='-')) p->i++;
    while(p->i<p->n&&p->z[p->i]>='0'&&p->z[p->i]<='9') p->i++;
  }
  len=p->i-start;
  if(len<=0||len>=64) return SQLITE_ERROR;
  memcpy(buf,p->z+start,len); buf[len]='\0';

  if(isFloat){
    double d=strtod(buf,NULL);
    u8 b[9]; u64 bits; b[0]=MP_FLOAT64;
    memcpy(&bits,&d,8); mpWrite64(b+1,bits);
    mpBufAppend(out,b,9);
  } else {
    i64 v=(i64)strtoll(buf,NULL,10);
    if(v>=0){
      if(v<=0x7f)              mpBufAppend1(out,(u8)v);
      else if(v<=0xff)         { u8 b[2]={MP_UINT8,(u8)v}; mpBufAppend(out,b,2); }
      else if(v<=0xffff)       { u8 b[3]; b[0]=MP_UINT16; mpWrite16(b+1,(u16)v); mpBufAppend(out,b,3); }
      else if(v<=(i64)0xffffffff){ u8 b[5]; b[0]=MP_UINT32; mpWrite32(b+1,(u32)v); mpBufAppend(out,b,5); }
      else                     { u8 b[9]; b[0]=MP_UINT64; mpWrite64(b+1,(u64)v); mpBufAppend(out,b,9); }
    } else {
      if(v>=-32)               mpBufAppend1(out,(u8)(i64)v);
      else if(v>=-128)         { u8 b[2]={MP_INT8,(u8)(i64)v}; mpBufAppend(out,b,2); }
      else if(v>=-32768)       { u8 b[3]; b[0]=MP_INT16; mpWrite16(b+1,(u16)(i64)v); mpBufAppend(out,b,3); }
      else if(v>=(i64)-2147483648LL){ u8 b[5]; b[0]=MP_INT32; mpWrite32(b+1,(u32)(i64)v); mpBufAppend(out,b,5); }
      else                     { u8 b[9]; b[0]=MP_INT64; mpWrite64(b+1,(u64)(i64)v); mpBufAppend(out,b,9); }
    }
  }
  return SQLITE_OK;
}

static int mpJpParseArray(MpJsonParser *p, MpBuf *out){
  MpBuf tmp; u32 count=0;
  mpBufInit(&tmp,out->pCtx);
  p->i++; /* skip '[' */
  mpJpSkipWS(p);
  while(p->i<p->n && p->z[p->i]!=']'){
    if(count>0){
      mpJpSkipWS(p);
      if(p->i>=p->n||p->z[p->i]!=','){ mpBufReset(&tmp); return SQLITE_ERROR; }
      p->i++;
    }
    mpJpSkipWS(p);
    if(mpJpParseValue(p,&tmp)!=SQLITE_OK){ mpBufReset(&tmp); return SQLITE_ERROR; }
    count++;
    mpJpSkipWS(p);
  }
  if(p->i>=p->n){ mpBufReset(&tmp); return SQLITE_ERROR; }
  p->i++; /* skip ']' */
  mpEncodeArrayHeader(out,count);
  mpBufAppend(out,tmp.aBuf,tmp.nUsed);
  mpBufReset(&tmp);
  return SQLITE_OK;
}

static int mpJpParseObject(MpJsonParser *p, MpBuf *out){
  MpBuf tmp; u32 count=0;
  mpBufInit(&tmp,out->pCtx);
  p->i++; /* skip '{' */
  mpJpSkipWS(p);
  while(p->i<p->n && p->z[p->i]!='}'){
    if(count>0){
      mpJpSkipWS(p);
      if(p->i>=p->n||p->z[p->i]!=','){ mpBufReset(&tmp); return SQLITE_ERROR; }
      p->i++;
    }
    mpJpSkipWS(p);
    if(p->i>=p->n||p->z[p->i]!='"'){ mpBufReset(&tmp); return SQLITE_ERROR; }
    if(mpJpParseString(p,&tmp)!=SQLITE_OK){ mpBufReset(&tmp); return SQLITE_ERROR; }
    mpJpSkipWS(p);
    if(p->i>=p->n||p->z[p->i]!=':'){ mpBufReset(&tmp); return SQLITE_ERROR; }
    p->i++;
    mpJpSkipWS(p);
    if(mpJpParseValue(p,&tmp)!=SQLITE_OK){ mpBufReset(&tmp); return SQLITE_ERROR; }
    count++;
    mpJpSkipWS(p);
  }
  if(p->i>=p->n){ mpBufReset(&tmp); return SQLITE_ERROR; }
  p->i++; /* skip '}' */
  mpEncodeMapHeader(out,count);
  mpBufAppend(out,tmp.aBuf,tmp.nUsed);
  mpBufReset(&tmp);
  return SQLITE_OK;
}

static int mpJpParseValue(MpJsonParser *p, MpBuf *out){
  mpJpSkipWS(p);
  if(p->i>=p->n) return SQLITE_ERROR;
  char c=p->z[p->i];
  if(c=='n'&&p->i+4<=p->n&&memcmp(p->z+p->i,"null",4)==0){ p->i+=4; mpBufAppend1(out,MP_NIL);   return SQLITE_OK; }
  if(c=='t'&&p->i+4<=p->n&&memcmp(p->z+p->i,"true",4)==0){ p->i+=4; mpBufAppend1(out,MP_TRUE);  return SQLITE_OK; }
  if(c=='f'&&p->i+5<=p->n&&memcmp(p->z+p->i,"false",5)==0){ p->i+=5; mpBufAppend1(out,MP_FALSE); return SQLITE_OK; }
  if(c=='"') return mpJpParseString(p,out);
  if(c=='[') return mpJpParseArray(p,out);
  if(c=='{') return mpJpParseObject(p,out);
  if(c=='-'||(c>='0'&&c<='9')) return mpJpParseNumber(p,out);
  return SQLITE_ERROR;
}

/* ---- Phase 5 SQL functions ---- */

static void msgpackToJsonFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  const u8 *a; u32 n;
  (void)argc;
  if(sqlite3_value_type(argv[0])!=SQLITE_BLOB){ sqlite3_result_null(ctx); return; }
  a=(const u8*)sqlite3_value_blob(argv[0]); n=(u32)sqlite3_value_bytes(argv[0]);
  MpBuf out; mpBufInit(&out,ctx);
  mpToJsonAt(&out,a,n,0, 0,0,0);
  mpBufAppend1(&out,0); /* NUL terminator */
  u32 nRes; u8 *res=mpBufFinish(&out,&nRes);
  if(res) sqlite3_result_text(ctx,(char*)res,(int)nRes-1,sqlite3_free);
}

static void msgpackPrettyFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  const u8 *a; u32 n;
  int indentW=2;
  if(sqlite3_value_type(argv[0])!=SQLITE_BLOB){ sqlite3_result_null(ctx); return; }
  if(argc>=2) indentW=sqlite3_value_int(argv[1]);
  if(indentW<0) indentW=0;
  if(indentW>8) indentW=8;
  a=(const u8*)sqlite3_value_blob(argv[0]); n=(u32)sqlite3_value_bytes(argv[0]);
  MpBuf out; mpBufInit(&out,ctx);
  mpToJsonAt(&out,a,n,0, 1,0,indentW);
  mpBufAppend1(&out,0);
  u32 nRes; u8 *res=mpBufFinish(&out,&nRes);
  if(res) sqlite3_result_text(ctx,(char*)res,(int)nRes-1,sqlite3_free);
}

static void msgpackFromJsonFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  const char *z; int n;
  (void)argc;
  if(sqlite3_value_type(argv[0])==SQLITE_NULL){ sqlite3_result_null(ctx); return; }
  z=(const char*)sqlite3_value_text(argv[0]);
  n=sqlite3_value_bytes(argv[0]);
  if(!z){ sqlite3_result_null(ctx); return; }
  MpJsonParser p; p.z=z; p.n=n; p.i=0;
  MpBuf out; mpBufInit(&out,ctx);
  if(mpJpParseValue(&p,&out)==SQLITE_OK){
    u32 nRes; u8 *res=mpBufFinish(&out,&nRes);
    if(res) sqlite3_result_blob(ctx,res,(int)nRes,sqlite3_free);
  } else {
    mpBufReset(&out);
    sqlite3_result_error(ctx,"msgpack_from_json: invalid JSON input",-1);
  }
}

/*
** -------------------------------------------------------
** Phase 6: Aggregate functions
** -------------------------------------------------------
*/

typedef struct MpAggState {
  u8  *aBuf;    /* accumulated encoded elements / pairs */
  u32  nBuf;    /* bytes used */
  u32  nAlloc;  /* bytes allocated */
  u32  nCount;  /* element / pair count */
  u8   bErr;
} MpAggState;

static void mpAggAppend(MpAggState *st, const u8 *data, u32 n, sqlite3_context *ctx){
  if(st->bErr) return;
  if(st->nBuf+n > st->nAlloc){
    u32 nNew=st->nAlloc ? st->nAlloc*2 : 512;
    while(nNew < st->nBuf+n) nNew*=2;
    u8 *aNew=st->nAlloc ? sqlite3_realloc(st->aBuf,nNew)
                        : sqlite3_malloc(nNew);
    if(!aNew){ st->bErr=1; sqlite3_result_error_nomem(ctx); return; }
    st->aBuf=aNew; st->nAlloc=nNew;
  }
  memcpy(st->aBuf+st->nBuf, data, n);
  st->nBuf+=n;
}

static void mpAggFreeState(MpAggState *st){
  if(st && st->aBuf){ sqlite3_free(st->aBuf); st->aBuf=0; }
}

/* msgpack_group_array */
static void msgpackGroupArrayStep(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  MpAggState *st=(MpAggState*)sqlite3_aggregate_context(ctx,sizeof(MpAggState));
  if(!st){ sqlite3_result_error_nomem(ctx); return; }
  (void)argc;
  MpBuf tmp; mpBufInit(&tmp,ctx);
  mpEncodeSqlValue(&tmp,argv[0]);
  if(!tmp.bErr){
    mpAggAppend(st, tmp.aBuf, tmp.nUsed, ctx);
    if(!st->bErr) st->nCount++;
  }
  mpBufReset(&tmp);
}

static void msgpackGroupArrayFinalize(sqlite3_context *ctx, int isFinal){
  MpAggState *st=(MpAggState*)sqlite3_aggregate_context(ctx,0);
  if(!st||st->bErr){ sqlite3_result_null(ctx); return; }
  MpBuf out; mpBufInit(&out,ctx);
  mpEncodeArrayHeader(&out, st->nCount);
  mpBufAppend(&out, st->aBuf, st->nBuf);
  u32 nRes; u8 *res=mpBufFinish(&out,&nRes);
  if(res) sqlite3_result_blob(ctx,res,(int)nRes,sqlite3_free);
  if(isFinal) mpAggFreeState(st);
}

static void msgpackGroupArrayFinal(sqlite3_context *ctx){
  msgpackGroupArrayFinalize(ctx,1);
}
static void msgpackGroupArrayValue(sqlite3_context *ctx){
  msgpackGroupArrayFinalize(ctx,0);
}
static void msgpackGroupArrayInverse(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  /* Simplified inverse: just decrement count. Correct for non-overlapping windows. */
  MpAggState *st=(MpAggState*)sqlite3_aggregate_context(ctx,0);
  (void)argc; (void)argv;
  if(st && st->nCount>0){
    /* Remove the first element from aBuf by re-skipping it */
    u32 skip=mpSkipOne(st->aBuf, st->nBuf, 0);
    if(skip && skip<=st->nBuf){
      memmove(st->aBuf, st->aBuf+skip, st->nBuf-skip);
      st->nBuf-=skip;
      st->nCount--;
    }
  }
}

/* msgpack_group_object */
static void msgpackGroupObjectStep(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  MpAggState *st=(MpAggState*)sqlite3_aggregate_context(ctx,sizeof(MpAggState));
  if(!st){ sqlite3_result_error_nomem(ctx); return; }
  (void)argc;
  MpBuf tmp; mpBufInit(&tmp,ctx);
  mpEncodeSqlValue(&tmp,argv[0]); /* key */
  mpEncodeSqlValue(&tmp,argv[1]); /* value */
  if(!tmp.bErr){
    mpAggAppend(st, tmp.aBuf, tmp.nUsed, ctx);
    if(!st->bErr) st->nCount++;
  }
  mpBufReset(&tmp);
}

static void msgpackGroupObjectFinalize(sqlite3_context *ctx, int isFinal){
  MpAggState *st=(MpAggState*)sqlite3_aggregate_context(ctx,0);
  if(!st||st->bErr){ sqlite3_result_null(ctx); return; }
  MpBuf out; mpBufInit(&out,ctx);
  mpEncodeMapHeader(&out, st->nCount);
  mpBufAppend(&out, st->aBuf, st->nBuf);
  u32 nRes; u8 *res=mpBufFinish(&out,&nRes);
  if(res) sqlite3_result_blob(ctx,res,(int)nRes,sqlite3_free);
  if(isFinal) mpAggFreeState(st);
}

static void msgpackGroupObjectFinal(sqlite3_context *ctx){
  msgpackGroupObjectFinalize(ctx,1);
}
static void msgpackGroupObjectValue(sqlite3_context *ctx){
  msgpackGroupObjectFinalize(ctx,0);
}
static void msgpackGroupObjectInverse(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  MpAggState *st=(MpAggState*)sqlite3_aggregate_context(ctx,0);
  (void)argc; (void)argv;
  if(st && st->nCount>0){
    /* Skip key + value */
    u32 skip=mpSkipOne(st->aBuf,st->nBuf,0);
    if(skip) skip=mpSkipOne(st->aBuf,st->nBuf,skip);
    if(skip && skip<=st->nBuf){
      memmove(st->aBuf, st->aBuf+skip, st->nBuf-skip);
      st->nBuf-=skip;
      st->nCount--;
    }
  }
}


/*
** -------------------------------------------------------
** Phase 7: Table-Valued Functions — msgpack_each / msgpack_tree
** -------------------------------------------------------
*/

/* Column indices */
#define MPVTAB_KEY      0
#define MPVTAB_VALUE    1
#define MPVTAB_TYPE     2
#define MPVTAB_ATOM     3
#define MPVTAB_ID       4
#define MPVTAB_PARENT   5
#define MPVTAB_FULLKEY  6
#define MPVTAB_PATH     7
#define MPVTAB_DATA     8   /* hidden */
#define MPVTAB_ROOT     9   /* hidden */

static const char *mpVtabSchema =
  "CREATE TABLE x("
  "  key,"
  "  value,"
  "  type     TEXT,"
  "  atom,"
  "  id       INTEGER,"
  "  parent   INTEGER,"
  "  fullkey  TEXT,"
  "  path     TEXT,"
  "  data     BLOB HIDDEN,"
  "  root     TEXT HIDDEN"
  ")";

typedef struct MpEachRow {
  u32 iOff, iEnd, iParent;   /* 0xffffffff = no parent */
  int keyType;               /* 0=root, 1=array-idx, 2=map-key */
  i64 iKey;
  const u8 *zKey; u32 nKey;  /* pointer into aBlob — valid while cursor open */
  char *zFullKey, *zParentPath;
} MpEachRow;

typedef struct MpEachVtab {
  sqlite3_vtab base;
  int bRecursive;
} MpEachVtab;

typedef struct MpEachCursor {
  sqlite3_vtab_cursor base;
  int bRecursive;
  u8  *aBlob; u32 nBlob;
  MpEachRow *aRows; int nRows, nAlloc, iCur;
} MpEachCursor;

static void mpVtabFreeRows(MpEachCursor *p){
  int i;
  for(i=0;i<p->nRows;i++){
    sqlite3_free(p->aRows[i].zFullKey);
    sqlite3_free(p->aRows[i].zParentPath);
  }
  sqlite3_free(p->aRows); p->aRows=0; p->nRows=0; p->nAlloc=0;
}

static int mpVtabAddRow(
  MpEachCursor *p,
  u32 iOff, u32 iEnd, u32 iParent,
  int keyType, i64 iKey,
  const u8 *zKey, u32 nKey,
  const char *zFull, const char *zPar
){
  MpEachRow *r;
  if(p->nRows>=p->nAlloc){
    int nN=p->nAlloc?p->nAlloc*2:16;
    MpEachRow *a=sqlite3_realloc(p->aRows,nN*sizeof(MpEachRow));
    if(!a) return SQLITE_NOMEM;
    p->aRows=a; p->nAlloc=nN;
  }
  r=&p->aRows[p->nRows++];
  r->iOff=iOff; r->iEnd=iEnd; r->iParent=iParent;
  r->keyType=keyType; r->iKey=iKey; r->zKey=zKey; r->nKey=nKey;
  r->zFullKey    =sqlite3_mprintf("%s",zFull?zFull:"$");
  r->zParentPath =sqlite3_mprintf("%s",zPar ?zPar :"$");
  if(!r->zFullKey||!r->zParentPath){
    sqlite3_free(r->zFullKey); sqlite3_free(r->zParentPath); p->nRows--;
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

/* Forward declaration */
static int mpVtabTreeWalk(MpEachCursor*,u32,u32,const char*,const char*,int,int,i64,const u8*,u32);

/* Iterate direct children of container at iCont (msgpack_each) */
static int mpVtabEachIter(MpEachCursor *p, u32 iCont, const char *zBase){
  const u8 *a=p->aBlob; u32 n=p->nBlob, j;
  int isArr=0,isMap=0; u32 count=0,dataOff=0;
  u8 b;
  if(iCont>=n) return SQLITE_OK;
  b=a[iCont];
  if(b>=0x90&&b<=0x9f)          {isArr=1;count=b&0x0f;         dataOff=iCont+1;}
  else if(b==MP_ARRAY16&&iCont+3<=n){isArr=1;count=mpRead16(a+iCont+1);dataOff=iCont+3;}
  else if(b==MP_ARRAY32&&iCont+5<=n){isArr=1;count=mpRead32(a+iCont+1);dataOff=iCont+5;}
  else if(b>=0x80&&b<=0x8f)     {isMap=1;count=b&0x0f;         dataOff=iCont+1;}
  else if(b==MP_MAP16&&iCont+3<=n)  {isMap=1;count=mpRead16(a+iCont+1);dataOff=iCont+3;}
  else if(b==MP_MAP32&&iCont+5<=n)  {isMap=1;count=mpRead32(a+iCont+1);dataOff=iCont+5;}
  if(!isArr&&!isMap) return SQLITE_OK;
  u32 cur=dataOff;
  for(j=0;j<count;j++){
    if(cur>=n) break;
    if(isArr){
      u32 cEnd=mpSkipOne(a,n,cur); if(!cEnd) break;
      char *zF=sqlite3_mprintf("%s[%u]",zBase,j); if(!zF) return SQLITE_NOMEM;
      int rc=mpVtabAddRow(p,cur,cEnd,iCont,1,(i64)j,0,0,zF,zBase);
      sqlite3_free(zF); if(rc) return rc; cur=cEnd;
    } else {
      u8 kb=a[cur]; const u8 *zKey=0; u32 nKey=0;
      if(kb>=0xa0&&kb<=0xbf)         {nKey=kb&0x1f;           zKey=a+cur+1;}
      else if(kb==MP_STR8 &&cur+2<=n){nKey=a[cur+1];           zKey=a+cur+2;}
      else if(kb==MP_STR16&&cur+3<=n){nKey=mpRead16(a+cur+1);  zKey=a+cur+3;}
      else if(kb==MP_STR32&&cur+5<=n){nKey=mpRead32(a+cur+1);  zKey=a+cur+5;}
      u32 vOff=mpSkipOne(a,n,cur); if(!vOff) break;
      u32 pEnd=mpSkipOne(a,n,vOff); if(!pEnd) break;
      char *zF=zKey?sqlite3_mprintf("%s.%.*s",zBase,(int)nKey,(const char*)zKey)
                   :sqlite3_mprintf("%s.?",zBase);
      if(!zF) return SQLITE_NOMEM;
      int rc=mpVtabAddRow(p,vOff,pEnd,iCont,2,0,zKey,nKey,zF,zBase);
      sqlite3_free(zF); if(rc) return rc; cur=pEnd;
    }
  }
  return SQLITE_OK;
}

/* DFS pre-order walk for msgpack_tree */
static int mpVtabTreeWalk(
  MpEachCursor *p, u32 iOff, u32 iParent,
  const char *zFull, const char *zParPath,
  int depth, int keyType, i64 iKey, const u8 *zKey, u32 nKey
){
  const u8 *a=p->aBlob; u32 n=p->nBlob, j;
  int isArr=0,isMap=0; u32 count=0,dataOff=0;
  u8 b;
  if(depth>64||iOff>=n) return SQLITE_OK;
  u32 iEnd=mpSkipOne(a,n,iOff); if(!iEnd) return SQLITE_OK;

  /* Yield this element */
  int rc=mpVtabAddRow(p,iOff,iEnd,iParent,keyType,iKey,zKey,nKey,zFull,zParPath);
  if(rc) return rc;

  b=a[iOff];
  if(b>=0x90&&b<=0x9f)          {isArr=1;count=b&0x0f;        dataOff=iOff+1;}
  else if(b==MP_ARRAY16&&iOff+3<=n){isArr=1;count=mpRead16(a+iOff+1);dataOff=iOff+3;}
  else if(b==MP_ARRAY32&&iOff+5<=n){isArr=1;count=mpRead32(a+iOff+1);dataOff=iOff+5;}
  else if(b>=0x80&&b<=0x8f)     {isMap=1;count=b&0x0f;        dataOff=iOff+1;}
  else if(b==MP_MAP16&&iOff+3<=n)  {isMap=1;count=mpRead16(a+iOff+1);dataOff=iOff+3;}
  else if(b==MP_MAP32&&iOff+5<=n)  {isMap=1;count=mpRead32(a+iOff+1);dataOff=iOff+5;}
  if(!isArr&&!isMap) return SQLITE_OK;

  u32 cur=dataOff;
  for(j=0;j<count;j++){
    if(cur>=n) break;
    if(isArr){
      u32 cEnd=mpSkipOne(a,n,cur); if(!cEnd) break;
      char *zC=sqlite3_mprintf("%s[%u]",zFull,j); if(!zC) return SQLITE_NOMEM;
      rc=mpVtabTreeWalk(p,cur,iOff,zC,zFull,depth+1,1,(i64)j,0,0);
      sqlite3_free(zC); if(rc) return rc; cur=cEnd;
    } else {
      u8 kb=a[cur]; const u8 *zCKey=0; u32 nCKey=0;
      if(kb>=0xa0&&kb<=0xbf)         {nCKey=kb&0x1f;           zCKey=a+cur+1;}
      else if(kb==MP_STR8 &&cur+2<=n){nCKey=a[cur+1];           zCKey=a+cur+2;}
      else if(kb==MP_STR16&&cur+3<=n){nCKey=mpRead16(a+cur+1);  zCKey=a+cur+3;}
      else if(kb==MP_STR32&&cur+5<=n){nCKey=mpRead32(a+cur+1);  zCKey=a+cur+5;}
      u32 vOff=mpSkipOne(a,n,cur); if(!vOff) break;
      u32 pEnd=mpSkipOne(a,n,vOff); if(!pEnd) break;
      char *zC=zCKey?sqlite3_mprintf("%s.%.*s",zFull,(int)nCKey,(const char*)zCKey)
                    :sqlite3_mprintf("%s.?",zFull);
      if(!zC) return SQLITE_NOMEM;
      rc=mpVtabTreeWalk(p,vOff,iOff,zC,zFull,depth+1,2,0,zCKey,nCKey);
      sqlite3_free(zC); if(rc) return rc; cur=pEnd;
    }
  }
  return SQLITE_OK;
}

/* ---- sqlite3_module callbacks ---- */

static int mpVtabConnect(
  sqlite3 *db, void *pAux, int argc, const char *const *argv,
  sqlite3_vtab **ppVtab, char **pzErr
){
  (void)argc;(void)argv;(void)pzErr;
  MpEachVtab *pNew=sqlite3_malloc(sizeof(MpEachVtab));
  if(!pNew) return SQLITE_NOMEM;
  memset(pNew,0,sizeof(*pNew));
  pNew->bRecursive=(int)(intptr_t)pAux;
  int rc=sqlite3_declare_vtab(db,mpVtabSchema);
  if(rc){sqlite3_free(pNew);return rc;}
  *ppVtab=(sqlite3_vtab*)pNew; return SQLITE_OK;
}
static int mpVtabDisconnect(sqlite3_vtab *p){sqlite3_free(p);return SQLITE_OK;}

static int mpVtabBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pIdx){
  int i; (void)pVtab;
  for(i=0;i<pIdx->nConstraint;i++){
    struct sqlite3_index_constraint *c=&pIdx->aConstraint[i];
    if(!c->usable) continue;
    if(c->iColumn==MPVTAB_DATA&&c->op==SQLITE_INDEX_CONSTRAINT_EQ)
      {pIdx->aConstraintUsage[i].argvIndex=1;pIdx->aConstraintUsage[i].omit=1;}
    if(c->iColumn==MPVTAB_ROOT&&c->op==SQLITE_INDEX_CONSTRAINT_EQ)
      {pIdx->aConstraintUsage[i].argvIndex=2;pIdx->aConstraintUsage[i].omit=1;}
  }
  pIdx->estimatedCost=10.0; pIdx->estimatedRows=10; return SQLITE_OK;
}

static int mpVtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  MpEachCursor *pCur=sqlite3_malloc(sizeof(MpEachCursor));
  if(!pCur) return SQLITE_NOMEM;
  memset(pCur,0,sizeof(*pCur));
  pCur->bRecursive=((MpEachVtab*)pVtab)->bRecursive;
  *ppCursor=(sqlite3_vtab_cursor*)pCur; return SQLITE_OK;
}

static int mpVtabClose(sqlite3_vtab_cursor *pCursor){
  MpEachCursor *p=(MpEachCursor*)pCursor;
  mpVtabFreeRows(p); sqlite3_free(p->aBlob); sqlite3_free(p); return SQLITE_OK;
}

static int mpVtabFilter(
  sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  MpEachCursor *pCur=(MpEachCursor*)pCursor;
  (void)idxNum;(void)idxStr;
  mpVtabFreeRows(pCur); sqlite3_free(pCur->aBlob);
  pCur->aBlob=0; pCur->nBlob=0; pCur->iCur=0;
  if(argc<1||sqlite3_value_type(argv[0])!=SQLITE_BLOB) return SQLITE_OK;
  const u8 *aD=(const u8*)sqlite3_value_blob(argv[0]);
  u32 nD=(u32)sqlite3_value_bytes(argv[0]);
  pCur->aBlob=sqlite3_malloc(nD>0?nD:1); if(!pCur->aBlob) return SQLITE_NOMEM;
  if(nD) memcpy(pCur->aBlob,aD,nD); pCur->nBlob=nD;

  u32 iRoot=0; const char *zBase="$";
  if(argc>=2&&sqlite3_value_type(argv[1])!=SQLITE_NULL){
    const char *zPath=(const char*)sqlite3_value_text(argv[1]);
    if(zPath&&zPath[0]=='$'){
      u32 iEnd=0;
      if(mpLookup(pCur->aBlob,nD,0,zPath,&iRoot,&iEnd)==SQLITE_OK) zBase=zPath;
    }
  }
  return pCur->bRecursive
    ? mpVtabTreeWalk(pCur,iRoot,0xffffffff,zBase,zBase,0,0,0,0,0)
    : mpVtabEachIter(pCur,iRoot,zBase);
}

static int mpVtabNext(sqlite3_vtab_cursor *pCursor){
  ((MpEachCursor*)pCursor)->iCur++; return SQLITE_OK;
}
static int mpVtabEof(sqlite3_vtab_cursor *pCursor){
  MpEachCursor *p=(MpEachCursor*)pCursor; return p->iCur>=p->nRows;
}
static int mpVtabRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid=(sqlite3_int64)((MpEachCursor*)pCursor)->iCur; return SQLITE_OK;
}

static int mpVtabColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int iCol){
  MpEachCursor *p=(MpEachCursor*)pCursor;
  if(p->iCur>=p->nRows){sqlite3_result_null(ctx);return SQLITE_OK;}
  MpEachRow *r=&p->aRows[p->iCur];
  const u8 *a=p->aBlob; u32 n=p->nBlob; const char *t;
  switch(iCol){
    case MPVTAB_KEY:
      if(r->keyType==0)      sqlite3_result_null(ctx);
      else if(r->keyType==1) sqlite3_result_int64(ctx,r->iKey);
      else if(r->zKey)       sqlite3_result_text(ctx,(const char*)r->zKey,(int)r->nKey,SQLITE_TRANSIENT);
      else                   sqlite3_result_null(ctx);
      break;
    case MPVTAB_VALUE:  mpReturnElement(ctx,a,n,r->iOff,r->iEnd); break;
    case MPVTAB_TYPE:   sqlite3_result_text(ctx,mpGetTypeStr(a,n,r->iOff),-1,SQLITE_STATIC); break;
    case MPVTAB_ATOM:
      t=mpGetTypeStr(a,n,r->iOff);
      if(strcmp(t,"array")==0||strcmp(t,"map")==0) sqlite3_result_null(ctx);
      else mpReturnElement(ctx,a,n,r->iOff,r->iEnd);
      break;
    case MPVTAB_ID:     sqlite3_result_int64(ctx,(i64)r->iOff); break;
    case MPVTAB_PARENT:
      if(r->iParent==0xffffffff) sqlite3_result_null(ctx);
      else sqlite3_result_int64(ctx,(i64)r->iParent);
      break;
    case MPVTAB_FULLKEY: sqlite3_result_text(ctx,r->zFullKey,-1,SQLITE_TRANSIENT);    break;
    case MPVTAB_PATH:    sqlite3_result_text(ctx,r->zParentPath,-1,SQLITE_TRANSIENT); break;
    default: sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static sqlite3_module mpVtabModule = {
  0,0, mpVtabConnect, mpVtabBestIndex, mpVtabDisconnect, 0,
  mpVtabOpen, mpVtabClose, mpVtabFilter, mpVtabNext,
  mpVtabEof, mpVtabColumn, mpVtabRowid,
  0,0,0,0,0,0,0,0,0,0,0
};

/* ── msgpack_version() ───────────────────────────────────────────────────── */

static void msgpackVersionFunc(
  sqlite3_context *ctx, int argc, sqlite3_value **argv
){
  (void)argc; (void)argv;
  sqlite3_result_text(ctx, SQLITE_MSGPACK_VERSION, -1, SQLITE_STATIC);
}

/*
** ============================================================
** Phase 8: Schema Validation — msgpack_schema_validate(mp, schema)
** ============================================================
*/

/* Read an integer value from msgpack at offset i. Returns 1 on success. */
static int mpReadInt64At(const u8 *a, u32 n, u32 i, i64 *pOut){
  u8 b;
  if( i>=n ) return 0;
  b = a[i];
  if( b<=0x7f ){ *pOut=(i64)b; return 1; }
  if( b>=0xe0 ){ *pOut=(i64)(signed char)b; return 1; }
  switch( b ){
    case MP_UINT8:  if(i+2<=n){ *pOut=(i64)a[i+1]; return 1; } break;
    case MP_UINT16: if(i+3<=n){ *pOut=(i64)mpRead16(a+i+1); return 1; } break;
    case MP_UINT32: if(i+5<=n){ *pOut=(i64)mpRead32(a+i+1); return 1; } break;
    case MP_UINT64: if(i+9<=n){ *pOut=(i64)mpRead64(a+i+1); return 1; } break;
    case MP_INT8:   if(i+2<=n){ *pOut=(i64)(signed char)a[i+1]; return 1; } break;
    case MP_INT16:  if(i+3<=n){ *pOut=(i64)(short)(int)mpRead16(a+i+1); return 1; } break;
    case MP_INT32:  if(i+5<=n){ *pOut=(i64)(int)mpRead32(a+i+1); return 1; } break;
    case MP_INT64:  if(i+9<=n){ *pOut=(i64)mpRead64(a+i+1); return 1; } break;
    default: break;
  }
  return 0;
}

/* Read a numeric value as double from msgpack at offset i. Returns 1 on success. */
static int mpReadDoubleAt(const u8 *a, u32 n, u32 i, double *pOut){
  i64 iv;
  u8 b;
  if( i>=n ) return 0;
  b = a[i];
  if( b==MP_FLOAT32 && i+5<=n ){
    u32 bits=mpRead32(a+i+1); float f; memcpy(&f,&bits,4);
    *pOut=(double)f; return 1;
  }
  if( b==MP_FLOAT64 && i+9<=n ){
    u64 bits=mpRead64(a+i+1); double d; memcpy(&d,&bits,8);
    *pOut=d; return 1;
  }
  if( mpReadInt64At(a,n,i,&iv) ){
    *pOut=(double)iv; return 1;
  }
  return 0;
}

/* Read a string pointer and length from msgpack at offset i. Returns 1 on success. */
static int mpReadStringAt(const u8 *a, u32 n, u32 i, const char **pStr, u32 *pLen){
  u8 b;
  u32 sLen, sOff;
  if( i>=n ) return 0;
  b = a[i];
  if( b>=0xa0 && b<=0xbf ){ sLen=b&0x1f; sOff=i+1; }
  else if( b==MP_STR8  && i+2<=n ){ sLen=a[i+1]; sOff=i+2; }
  else if( b==MP_STR16 && i+3<=n ){ sLen=mpRead16(a+i+1); sOff=i+3; }
  else if( b==MP_STR32 && i+5<=n ){ sLen=mpRead32(a+i+1); sOff=i+5; }
  else return 0;
  if( sOff>n || sLen>n-sOff ) return 0;
  *pLen = sLen;
  *pStr = (const char*)(a+sOff);
  return 1;
}

/* Get array element count and data offset. Returns 1 on success. */
static int mpArrayInfo(const u8 *a, u32 n, u32 i, u32 *pCount, u32 *pDataOff){
  u8 b;
  if( i>=n ) return 0;
  b = a[i];
  if( b>=0x90 && b<=0x9f ){ *pCount=b&0x0f; *pDataOff=i+1; return 1; }
  if( b==MP_ARRAY16 && i+3<=n ){ *pCount=mpRead16(a+i+1); *pDataOff=i+3; return 1; }
  if( b==MP_ARRAY32 && i+5<=n ){ *pCount=mpRead32(a+i+1); *pDataOff=i+5; return 1; }
  return 0;
}

/* Get map pair count and data offset. Returns 1 on success. */
static int mpMapInfo(const u8 *a, u32 n, u32 i, u32 *pCount, u32 *pDataOff){
  u8 b;
  if( i>=n ) return 0;
  b = a[i];
  if( b>=0x80 && b<=0x8f ){ *pCount=b&0x0f; *pDataOff=i+1; return 1; }
  if( b==MP_MAP16 && i+3<=n ){ *pCount=mpRead16(a+i+1); *pDataOff=i+3; return 1; }
  if( b==MP_MAP32 && i+5<=n ){ *pCount=mpRead32(a+i+1); *pDataOff=i+5; return 1; }
  return 0;
}

/* Find a string key in a msgpack map. Returns offset to value, or 0 if not found. */
static u32 mpMapFindKey(const u8 *a, u32 n, u32 iMap, const char *key, u32 keyLen){
  u32 count, off, j;
  if( !mpMapInfo(a,n,iMap,&count,&off) ) return 0;
  for( j=0; j<count; j++ ){
    const char *kStr; u32 kLen;
    u32 valOff = mpSkipOne(a,n,off);
    if( !valOff || valOff>=n ) return 0;
    if( mpReadStringAt(a,n,off,&kStr,&kLen)
        && kLen==keyLen && memcmp(kStr,key,keyLen)==0 ){
      return valOff;
    }
    off = mpSkipOne(a,n,valOff);
    if( !off && j<count-1 ) return 0;
  }
  return 0;
}

/* Count UTF-8 codepoints in a byte string */
static u32 mpUtf8CpCount(const char *s, u32 nBytes){
  u32 count=0, i;
  for( i=0; i<nBytes; ){
    u8 b=(u8)s[i];
    if( b<0x80 ) i+=1;
    else if( (b&0xe0)==0xc0 ) i+=2;
    else if( (b&0xf0)==0xe0 ) i+=3;
    else if( (b&0xf8)==0xf0 ) i+=4;
    else i+=1;
    count++;
  }
  return count;
}

/* Check if a msgpack data type matches a schema type name */
static int mpSchemaTypeMatch(const char *dataType, const char *schemaType, u32 stLen){
  u32 dtLen;
  if( stLen==3 && memcmp(schemaType,"any",3)==0 ) return 1;
  if( stLen==4 && memcmp(schemaType,"bool",4)==0 ){
    return (strcmp(dataType,"true")==0 || strcmp(dataType,"false")==0);
  }
  dtLen=(u32)strlen(dataType);
  return (dtLen==stLen && memcmp(dataType,schemaType,dtLen)==0);
}

/* Compare two msgpack values for equality (byte-level on encoded form) */
static int mpValuesEqual(
  const u8 *a1, u32 n1, u32 i1,
  const u8 *a2, u32 n2, u32 i2
){
  u32 end1=mpSkipOne(a1,n1,i1);
  u32 end2=mpSkipOne(a2,n2,i2);
  u32 len1, len2;
  if( !end1 || !end2 ) return 0;
  len1=end1-i1;
  len2=end2-i2;
  if( len1!=len2 ) return 0;
  return memcmp(a1+i1,a2+i2,len1)==0;
}

/*
** Core recursive schema validator.
** Returns 1 if data at iData conforms to schema at iSchema, 0 otherwise.
*/
static int mpSchemaValidateAt(
  const u8 *data, u32 nData, u32 iData,
  const u8 *schema, u32 nSchema, u32 iSchema,
  int depth
){
  const char *dataType;
  u32 off;

  if( depth>MP_MAX_DEPTH ) return 0;
  if( iSchema>=nSchema ) return 1;

  /* Boolean schema: true → always valid, false → never valid */
  if( schema[iSchema]==MP_TRUE ) return 1;
  if( schema[iSchema]==MP_FALSE ) return 0;

  /* Schema must be a map; empty or non-map schema validates anything */
  { u32 sCount, sOff;
    if( !mpMapInfo(schema,nSchema,iSchema,&sCount,&sOff) ) return 1;
    if( sCount==0 ) return 1;
  }

  dataType = mpGetTypeStr(data,nData,iData);

  /* ── "type" keyword ───────────────────────────────────────────── */
  off = mpMapFindKey(schema,nSchema,iSchema,"type",4);
  if( off ){
    const char *tStr; u32 tLen;
    if( mpReadStringAt(schema,nSchema,off,&tStr,&tLen) ){
      if( !mpSchemaTypeMatch(dataType,tStr,tLen) ) return 0;
    } else {
      u32 tCount, tDataOff;
      if( mpArrayInfo(schema,nSchema,off,&tCount,&tDataOff) ){
        int matched=0; u32 j, cur=tDataOff;
        for( j=0; j<tCount && !matched; j++ ){
          const char *ts; u32 tsLen;
          if( mpReadStringAt(schema,nSchema,cur,&ts,&tsLen) ){
            if( mpSchemaTypeMatch(dataType,ts,tsLen) ) matched=1;
          }
          cur=mpSkipOne(schema,nSchema,cur);
          if( !cur ) break;
        }
        if( !matched ) return 0;
      }
    }
  }

  /* ── "const" keyword ──────────────────────────────────────────── */
  off = mpMapFindKey(schema,nSchema,iSchema,"const",5);
  if( off ){
    if( !mpValuesEqual(data,nData,iData,schema,nSchema,off) ) return 0;
  }

  /* ── "enum" keyword ───────────────────────────────────────────── */
  off = mpMapFindKey(schema,nSchema,iSchema,"enum",4);
  if( off ){
    u32 eCount, eDataOff;
    if( mpArrayInfo(schema,nSchema,off,&eCount,&eDataOff) ){
      int matched=0; u32 j, cur=eDataOff;
      for( j=0; j<eCount && !matched; j++ ){
        if( mpValuesEqual(data,nData,iData,schema,nSchema,cur) ) matched=1;
        cur=mpSkipOne(schema,nSchema,cur);
        if( !cur ) break;
      }
      if( !matched ) return 0;
    }
  }

  /* ── Numeric constraints (integer and real) ───────────────────── */
  if( strcmp(dataType,"integer")==0 || strcmp(dataType,"real")==0 ){
    double dv;
    if( mpReadDoubleAt(data,nData,iData,&dv) ){
      double sv;
      off = mpMapFindKey(schema,nSchema,iSchema,"minimum",7);
      if( off && mpReadDoubleAt(schema,nSchema,off,&sv) ){
        if( dv<sv ) return 0;
      }
      off = mpMapFindKey(schema,nSchema,iSchema,"maximum",7);
      if( off && mpReadDoubleAt(schema,nSchema,off,&sv) ){
        if( dv>sv ) return 0;
      }
      off = mpMapFindKey(schema,nSchema,iSchema,"exclusiveMinimum",16);
      if( off && mpReadDoubleAt(schema,nSchema,off,&sv) ){
        if( dv<=sv ) return 0;
      }
      off = mpMapFindKey(schema,nSchema,iSchema,"exclusiveMaximum",16);
      if( off && mpReadDoubleAt(schema,nSchema,off,&sv) ){
        if( dv>=sv ) return 0;
      }
    }
  }

  /* ── Text constraints ─────────────────────────────────────────── */
  if( strcmp(dataType,"text")==0 ){
    const char *str; u32 strLen;
    if( mpReadStringAt(data,nData,iData,&str,&strLen) ){
      i64 sv;
      off = mpMapFindKey(schema,nSchema,iSchema,"minLength",9);
      if( off && mpReadInt64At(schema,nSchema,off,&sv) ){
        if( (i64)mpUtf8CpCount(str,strLen)<sv ) return 0;
      }
      off = mpMapFindKey(schema,nSchema,iSchema,"maxLength",9);
      if( off && mpReadInt64At(schema,nSchema,off,&sv) ){
        if( (i64)mpUtf8CpCount(str,strLen)>sv ) return 0;
      }
    }
  }

  /* ── Array constraints ────────────────────────────────────────── */
  if( strcmp(dataType,"array")==0 ){
    u32 aCount, aDataOff;
    if( mpArrayInfo(data,nData,iData,&aCount,&aDataOff) ){
      i64 sv;
      off = mpMapFindKey(schema,nSchema,iSchema,"minItems",8);
      if( off && mpReadInt64At(schema,nSchema,off,&sv) ){
        if( (i64)aCount<sv ) return 0;
      }
      off = mpMapFindKey(schema,nSchema,iSchema,"maxItems",8);
      if( off && mpReadInt64At(schema,nSchema,off,&sv) ){
        if( (i64)aCount>sv ) return 0;
      }
      off = mpMapFindKey(schema,nSchema,iSchema,"items",5);
      if( off ){
        if( schema[off]==MP_FALSE ){
          if( aCount>0 ) return 0;
        } else if( schema[off]!=MP_TRUE ){
          u32 j, cur=aDataOff;
          for( j=0; j<aCount; j++ ){
            if( !mpSchemaValidateAt(data,nData,cur,schema,nSchema,off,depth+1) )
              return 0;
            cur=mpSkipOne(data,nData,cur);
            if( !cur && j<aCount-1 ) return 0;
          }
        }
      }
    }
  }

  /* ── Map constraints ──────────────────────────────────────────── */
  if( strcmp(dataType,"map")==0 ){
    u32 mCount, mDataOff;
    if( mpMapInfo(data,nData,iData,&mCount,&mDataOff) ){
      u32 propsOff, reqOff, addPropsOff;

      /* required: check that all required keys exist in data */
      reqOff = mpMapFindKey(schema,nSchema,iSchema,"required",8);
      if( reqOff ){
        u32 rCount, rDataOff;
        if( mpArrayInfo(schema,nSchema,reqOff,&rCount,&rDataOff) ){
          u32 j, cur=rDataOff;
          for( j=0; j<rCount; j++ ){
            const char *rKey; u32 rKeyLen;
            if( mpReadStringAt(schema,nSchema,cur,&rKey,&rKeyLen) ){
              if( !mpMapFindKey(data,nData,iData,rKey,rKeyLen) ) return 0;
            }
            cur=mpSkipOne(schema,nSchema,cur);
            if( !cur && j<rCount-1 ) return 0;
          }
        }
      }

      /* properties + additionalProperties: validate each data key/value */
      propsOff = mpMapFindKey(schema,nSchema,iSchema,"properties",10);
      addPropsOff = mpMapFindKey(schema,nSchema,iSchema,"additionalProperties",20);
      {
        u32 j, cur=mDataOff;
        for( j=0; j<mCount; j++ ){
          const char *kStr; u32 kLen;
          u32 valOff = mpSkipOne(data,nData,cur);
          if( !valOff ) return 0;

          if( mpReadStringAt(data,nData,cur,&kStr,&kLen) ){
            int isKnown = 0;
            if( propsOff ){
              u32 propSchema = mpMapFindKey(schema,nSchema,propsOff,kStr,kLen);
              if( propSchema ){
                isKnown = 1;
                if( !mpSchemaValidateAt(data,nData,valOff,
                      schema,nSchema,propSchema,depth+1) )
                  return 0;
              }
            }
            if( !isKnown && addPropsOff ){
              if( schema[addPropsOff]==MP_FALSE ) return 0;
              if( schema[addPropsOff]!=MP_TRUE ){
                if( !mpSchemaValidateAt(data,nData,valOff,
                      schema,nSchema,addPropsOff,depth+1) )
                  return 0;
              }
            }
          }

          cur = mpSkipOne(data,nData,valOff);
          if( !cur && j<mCount-1 ) return 0;
        }
      }
    }
  }

  return 1;
}

/* Schema cache for sqlite3_set_auxdata — avoids re-parsing constant schemas */
typedef struct MpSchemaCache {
  u8 *aSchema;
  u32 nSchema;
} MpSchemaCache;

static void mpSchemaCacheFree(void *p){
  MpSchemaCache *sc = (MpSchemaCache*)p;
  if( sc ){
    sqlite3_free(sc->aSchema);
    sqlite3_free(sc);
  }
}

/*
** msgpack_schema_validate(mp, schema)
**   Returns 1 if mp conforms to schema, 0 otherwise.
**   schema can be JSON text or a msgpack BLOB.
*/
static void msgpackSchemaValidateFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *data;
  u32 nData;
  const u8 *schema = NULL;
  u32 nSchema = 0;
  int result;
  MpSchemaCache *cached;
  u8 *localSchema = NULL;

  (void)argc;

  /* First arg: msgpack data BLOB */
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    sqlite3_result_null(ctx); return;
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_BLOB ){
    sqlite3_result_int(ctx, 0); return;
  }
  data = (const u8*)sqlite3_value_blob(argv[0]);
  nData = (u32)sqlite3_value_bytes(argv[0]);
  if( !data || !nData ){ sqlite3_result_int(ctx, 0); return; }

  /* Second arg: schema (TEXT or BLOB) */
  if( sqlite3_value_type(argv[1])==SQLITE_NULL ){
    sqlite3_result_null(ctx); return;
  }

  cached = (MpSchemaCache*)sqlite3_get_auxdata(ctx, 1);
  if( cached ){
    schema = cached->aSchema;
    nSchema = cached->nSchema;
  } else if( sqlite3_value_type(argv[1])==SQLITE_TEXT ){
    const char *zJson = (const char*)sqlite3_value_text(argv[1]);
    int nJson = sqlite3_value_bytes(argv[1]);
    MpBuf buf;
    MpJsonParser jp;
    u32 nParsed;

    if( !zJson ){ sqlite3_result_error_nomem(ctx); return; }
    mpBufInit(&buf, NULL);
    jp.z = zJson; jp.n = nJson; jp.i = 0;
    if( mpJpParseValue(&jp, &buf)!=SQLITE_OK ){
      mpBufReset(&buf);
      sqlite3_result_error(ctx,
        "msgpack_schema_validate: invalid JSON schema", -1);
      return;
    }
    localSchema = mpBufFinish(&buf, &nParsed);
    if( !localSchema ){ sqlite3_result_error_nomem(ctx); return; }
    schema = localSchema;
    nSchema = nParsed;
  } else if( sqlite3_value_type(argv[1])==SQLITE_BLOB ){
    schema = (const u8*)sqlite3_value_blob(argv[1]);
    nSchema = (u32)sqlite3_value_bytes(argv[1]);
  } else {
    sqlite3_result_error(ctx,
      "msgpack_schema_validate: schema must be TEXT or BLOB", -1);
    return;
  }

  if( !schema || !nSchema ){
    sqlite3_result_int(ctx, 0);
    sqlite3_free(localSchema);
    return;
  }

  result = mpSchemaValidateAt(data, nData, 0, schema, nSchema, 0, 0);
  sqlite3_result_int(ctx, result);

  /* Cache parsed schema for subsequent calls (copy to avoid ownership issues) */
  if( localSchema && !cached ){
    u8 *copy = (u8*)sqlite3_malloc(nSchema);
    if( copy ){
      MpSchemaCache *sc = (MpSchemaCache*)sqlite3_malloc(sizeof(MpSchemaCache));
      if( sc ){
        memcpy(copy, localSchema, nSchema);
        sc->aSchema = copy;
        sc->nSchema = nSchema;
        sqlite3_set_auxdata(ctx, 1, sc, mpSchemaCacheFree);
      } else {
        sqlite3_free(copy);
      }
    }
  }
  sqlite3_free(localSchema);
}

#ifdef _WIN32
__declspec(dllexport)
#elif defined(__GNUC__)
__attribute__((visibility("default")))
#endif
int sqlite3_msgpack_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;

  /* Phase 1 */
  rc = sqlite3_create_function_v2(db, "msgpack_valid", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackValidFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_valid", 2,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackValidFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_quote", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackQuoteFunc, 0, 0, 0);
  if( rc ) return rc;

  /* Phase 2: Construction */
  rc = sqlite3_create_function_v2(db, "msgpack", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_array", -1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackArrayFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_object", -1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackObjectFunc, 0, 0, 0);
  if( rc ) return rc;

  /* Phase 3: Extraction */
  rc = sqlite3_create_function_v2(db, "msgpack_extract", -1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackExtractFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_type", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackTypeFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_type", 2,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackTypeFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_array_length", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackArrayLengthFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_array_length", 2,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackArrayLengthFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_error_position", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackErrorPositionFunc, 0, 0, 0);
  if( rc ) return rc;

  /* Phase 4: Mutation */
  rc = sqlite3_create_function_v2(db, "msgpack_set", -1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackSetFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_insert", -1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackInsertFunc2, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_replace", -1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackReplaceFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_remove", -1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackRemoveFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_array_insert", 3,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackArrayInsertFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_patch", 2,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackPatchFunc, 0, 0, 0);
  if( rc ) return rc;

  /* Phase 5: Conversion */
  rc = sqlite3_create_function_v2(db, "msgpack_to_json", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackToJsonFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_to_jsonb", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackToJsonFunc, 0, 0, 0); /* alias — returns TEXT */
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_pretty", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackPrettyFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_pretty", 2,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackPrettyFunc, 0, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function_v2(db, "msgpack_from_json", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackFromJsonFunc, 0, 0, 0);
  if( rc ) return rc;

  /* Phase 6: Aggregates */
  rc = sqlite3_create_window_function(db, "msgpack_group_array", 1,
         SQLITE_UTF8|SQLITE_INNOCUOUS,
         0,
         msgpackGroupArrayStep,
         msgpackGroupArrayFinal,
         msgpackGroupArrayValue,
         msgpackGroupArrayInverse,
         0);
  if( rc ) return rc;
  rc = sqlite3_create_window_function(db, "msgpack_group_object", 2,
         SQLITE_UTF8|SQLITE_INNOCUOUS,
         0,
         msgpackGroupObjectStep,
         msgpackGroupObjectFinal,
         msgpackGroupObjectValue,
         msgpackGroupObjectInverse,
         0);
  if( rc ) return rc;

  /* Phase 7: Table-valued functions */
  rc = sqlite3_create_module_v2(db, "msgpack_each", &mpVtabModule,
         (void*)0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_module_v2(db, "msgpack_tree", &mpVtabModule,
         (void*)1, 0);
  if( rc ) return rc;

  /* Version scalar */
  rc = sqlite3_create_function_v2(db, "msgpack_version", 0,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackVersionFunc, 0, 0, 0);
  if( rc ) return rc;

  /* Phase 8: Schema validation */
  rc = sqlite3_create_function_v2(db, "msgpack_schema_validate", 2,
         SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
         0, msgpackSchemaValidateFunc, 0, 0, 0);
  if( rc ) return rc;

  return SQLITE_OK;
}
