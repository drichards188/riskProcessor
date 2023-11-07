import retrieve_data
import processor


if __name__ == '__main__':
    # processor.handler({"msg": "hiya"}, None)
    symbols = retrieve_data.get_symbol_list("/home/drich/financedata/symbol_list.txt")
    if symbols:
        symbol_data = retrieve_data.handler({"symbols": symbols}, None)
        json_data = processor.get_json(symbols)
        print(f'--> symbol_data is: {symbol_data}')
    else:
        print("No symbols in list")
