import logUtils
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
