import logUtils

import logging
import multiprocessing
import traceback
from contextlib import closing

# some generic stuff handy for using in conjunction with the multiprocessing library:

def childProcessMain(logConfig, logQueue, mainMethod, args=(), kwargs={}):
    # setup the logging:
    try:
        logUtils.configureWorkerProcess(logConfig, logQueue)

        mainMethod(*args, **kwargs)

    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print('Problem with Process: ')
        traceback.print_exc()

    return

# for using with multiprocessing.Pool, starts up the logging for each Process it starts, like the above function,
# but just doesnt need a "mainMethod" to run, once the initialiser is done, the Pool will manage
# its lifetime
def poolProcessInitialiser(logConfig, logQueue):
    logUtils.configureWorkerProcess(logConfig, logQueue)

    logging.getLogger(__name__).info('Pool Process Initialised')

    return 

# If numProcesses is not provided, it will check the number of cores on the
# machine and then spawn the number of processes to match.
def createMultiprocessPool(numProcesses=None):
    return multiprocessing.Pool(
        processes=numProcesses,
        initializer=poolProcessInitialiser,
        initargs=(logUtils.logConfig, logUtils.logQueue))

def joinPool(pool):
    # stop them from receiving anymore jobs...
    pool.close()
    # now wait for them to join
    pool.join()
