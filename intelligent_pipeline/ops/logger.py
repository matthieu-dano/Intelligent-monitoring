from functools import wraps

import pandas as pd


def logging_decorator(func):
    @wraps(func)
    def wrapper(context, *args, **kwargs):
        # Get the logger from the context
        logger = context.log
        # Log the start of the function
        logger.info(f"Starting {func.__name__}")
        # Call the original function and get its result
        result = func(context, *args, **kwargs)
        logger.info(f"Result: {result}")
        # Check if the result is a DataFrame and log if it's empty
        if isinstance(result, pd.DataFrame) and result.empty:
            logger.warning("Processed data is empty.")
        else:
            # Log the completion of the function
            logger.info(f"Finished {func.__name__}")
        return result

    return wrapper
