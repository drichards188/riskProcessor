import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    try:
        logger.info('## EVENT')
        logger.info(event)
        if "symbols" in event:
            for symbol in event["symbols"]:
                json_data = {"symbol": symbol}
                json_str = json.dumps(json_data)

                f = open(f"/home/drich/financedata/{symbol}.json", "w")
                f.write(json_str)
                f.close()
            return {"response": event["symbols"]}
        return {"response": "No symbols found"}
    except Exception as e:
        logger.error(e)
        raise e


def get_symbol_list(filepath: str):
    symbols = []

    try:
        f = open(filepath, "r")
        for lines in f:
            symbols.append(lines.strip())
    except Exception as e:
        print(f'error: {e}')

    return symbols
