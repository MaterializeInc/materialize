/* Retrieved from https://github.com/pmwkaa/ioarena.
 *
 * Copyright (C) ioarena authors
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <assert.h>
#include <errno.h>

#include "pthread_shim.h"

#ifndef _POSIX_BARRIERS

int pthread_barrier_init(pthread_barrier_t *barrier,
                         const pthread_barrierattr_t *attr, unsigned count) {
  if (count == 0) {
    errno = EINVAL;
    return -1;
  }
  if (attr != NULL) {
    errno = ENOSYS;
    return -1;
  }

  if (pthread_mutex_init(&barrier->mutex, NULL) < 0) {
    return -1;
  }
  if (pthread_cond_init(&barrier->cond, NULL) < 0) {
    pthread_mutex_destroy(&barrier->mutex);
    return -1;
  }

  barrier->threshold = count;
  barrier->canary = 0;
  return 0;
}

int pthread_barrier_destroy(pthread_barrier_t *barrier) {
  barrier->threshold = -1;
  pthread_cond_destroy(&barrier->cond);
  return pthread_mutex_destroy(&barrier->mutex);
}

int pthread_barrier_wait(pthread_barrier_t *barrier) {
  int rc = pthread_mutex_lock(&barrier->mutex);
  if (rc == 0) {
    assert(barrier->threshold > 0);
    assert(barrier->canary >= 0 && barrier->canary < barrier->threshold);

    if (++barrier->canary == barrier->threshold) {
      barrier->canary = 0;
      pthread_cond_broadcast(&barrier->cond);
      rc = PTHREAD_BARRIER_SERIAL_THREAD;
    } else {
      pthread_cond_wait(&barrier->cond, &barrier->mutex);
    }

    pthread_mutex_unlock(&barrier->mutex);
  }
  return rc;
}

#endif // !defined(_POSIX_BARRIERS)