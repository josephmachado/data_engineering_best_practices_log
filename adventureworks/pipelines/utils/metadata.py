import inspect
from utils.db import MetadatadbConnection


def log_metadata(func):
    def log_wrapper(*args, **kwargs):
        input_params = dict(
            zip(list(locals().keys())[:-1], list(locals().values())[:-1])
        )
        param_names = list(
            inspect.signature(func).parameters.keys()
        )  # order is preserved
        # match with input_params.get('args') and
        # then input_params.get('kwargs')
        input_dict = {}
        for v in input_params.get("args"):
            input_dict[param_names.pop(0)] = v

        # We insert the runs unique id, name of the pipeline, & all the inputs to the
        # function
        run_id = input_params.get("kwargs").get("run_id")
        pipeline_id = input_params.get("kwargs").get("pipeline_id")
        run_params = str(
            input_dict | input_params.get("kwargs") | {"function": func.__name__}
        )

        db_conn = MetadatadbConnection()
        with db_conn.managed_cursor() as cur:
            insert_query = """
            INSERT INTO run_metadata (run_id, pipeline_id, run_params)
            VALUES (%s, %s, %s);
            """
            cur.execute(insert_query, (run_id, pipeline_id, run_params))

        return func(*args, **kwargs)

    return log_wrapper
