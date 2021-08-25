/*
 * Copyright © 2006 Robert Millan
 * Copyright © 2010-2012 Guillem Jover <guillem@hadrons.org>
 * Copyright © 2018 Facebook, Inc.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * Rejected in glibc
 * <https://sourceware.org/ml/libc-alpha/2006-03/msg00125.html>.
 */

#include <sys/param.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#ifdef _WIN32
#include <Windows.h>
#include <shlwapi.h>
#endif

#ifdef _WIN32
#define LIBBSD_IS_PATHNAME_SEPARATOR(c) ((c) == '/' || (c) == '\\')
#else
#define LIBBSD_IS_PATHNAME_SEPARATOR(c) ((c) == '/')
#endif

#ifdef HAVE___PROGNAME
extern const char *__progname;
#else
static const char *__progname = NULL;
#endif

const char *
getprogname(void)
{
#if defined(HAVE_PROGRAM_INVOCATION_SHORT_NAME)
	if (__progname == NULL)
		__progname = program_invocation_short_name;
#elif defined(HAVE_GETEXECNAME)
	/* getexecname(3) returns an absolute pathname, normalize it. */
	if (__progname == NULL)
		setprogname(getexecname());
#elif defined(_WIN32)
	if (__progname == NULL) {
		WCHAR *wpath = NULL;
		WCHAR *wname = NULL;
		WCHAR *wext = NULL;
		DWORD wpathsiz = MAX_PATH / 2;
		DWORD len, i;
		char *mbname = NULL;
		int mbnamesiz;

		/* Use the Unicode version of this function to support long
		 * paths. MAX_PATH isn't actually the maximum length of a
		 * path in this case. */
		do {
			WCHAR *wpathnew;

			wpathsiz *= 2;
			wpathsiz = MIN(wpathsiz, UNICODE_STRING_MAX_CHARS);
			wpathnew = reallocarray(wpath, wpathsiz, sizeof(*wpath));
			if (wpathnew == NULL)
				goto done;
			wpath = wpathnew;

			len = GetModuleFileNameW(NULL, wpath, wpathsiz);
			if (wpathsiz == UNICODE_STRING_MAX_CHARS)
				goto done;
		} while (wpathsiz == len);
		if (len == 0)
			goto done;

		/* GetModuleFileNameW() retrieves an absolute path. Locate the
		 * filename now to only convert necessary characters and save
		 * memory. */
		wname = wpath;
		for (i = len; i > 0; i--) {
			if (LIBBSD_IS_PATHNAME_SEPARATOR(wpath[i - 1])) {
				wname = wpath + i;
				break;
			}
		}

		/* Remove any trailing extension, such as '.exe', to make the
		 * behavior mach the non-Windows systems. */
		wext = PathFindExtensionW(wname);
		wext[0] = '\0';

		mbnamesiz = WideCharToMultiByte(CP_UTF8, 0, wname, -1, NULL,
		                                0, NULL, NULL);
		if (mbnamesiz == 0)
			goto done;
		mbname = malloc(mbnamesiz);
		if (mbname == NULL)
			goto done;
		mbnamesiz = WideCharToMultiByte(CP_UTF8, 0, wname, -1, mbname,
		                                mbnamesiz, NULL, NULL);
		if (mbnamesiz == 0)
			goto done;
		__progname = mbname;
		mbname = NULL;

done:
		free(wpath);
		free(mbname);
	}
#endif

	return __progname;
}

void
setprogname(const char *progname)
{
	size_t i;

	for (i = strlen(progname); i > 0; i--) {
		if (LIBBSD_IS_PATHNAME_SEPARATOR(progname[i - 1])) {
			__progname = progname + i;
			return;
		}
	}
	__progname = progname;
}
