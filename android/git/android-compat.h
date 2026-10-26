/* What git needs from POSIX that Android's C library (bionic) leaves out.
 * Included in every git source file by build.sh. */
#include <pthread.h>

/* bionic has no thread cancellation, so nothing can be cancelled: disabling
 * it around fork (run-command.c) is already the state of every thread. */
#ifndef PTHREAD_CANCEL_ENABLE
#define PTHREAD_CANCEL_ENABLE 0
#define PTHREAD_CANCEL_DISABLE 1
static inline int pthread_setcancelstate(int state, int *old)
{
	(void)state;
	if (old)
		*old = PTHREAD_CANCEL_DISABLE;
	return 0;
}
#endif
