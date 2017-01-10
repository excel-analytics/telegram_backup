#encoding:utf-8

from pytg import Telegram
import pymongo
from tqdm import tqdm


class TelegramBackuper(object):
    def __init__(self, binary, pubkey):
        super(TelegramBackuper, self).__init__()
        self.tg = Telegram(
            telegram=binary,
            pubkey_file=pubkey)
        self.sender = self.tg.sender
        self.content_collection = pymongo.MongoClient()['tg_backup']['content']
        self.metadata_collection = pymongo.MongoClient()['tg_backup']['metadata']
        
    def _history(self, chat, count, offset=0):
        return self.sender.history(chat, count, offset)

    def _gialogs(self, count, offset=0):
        return self.sender.dialog_list(count, offset)

    def get_gialogs(self, cnt, offset):
        self._store_metadata(self._gialogs(cnt, offset))

    def get_hist_for_id(self, chat_id):
        print_name = self.metadata_collection.find_one({'id': chat_id})['print_name']
        pbar = tqdm()
        meta_data = dict()
        content = list()
        for msg in self._history(print_name, 1000):
            content_part = dict(msg.copy())
            content_part['from'] = {'id': msg['from']['id']}
            meta_data[msg['from']['id']] = msg['from']
            content_part['to'] = {'id': msg['to']['id']}
            meta_data[msg['to']['id']] = msg['to']
            content.append(content_part)
            pbar.update(1)
        self._store_content(content)
        self._store_metadata(meta_data)

    def _store_content(self, content):
        self.content_collection.insert_many(content)

    def _store_metadata(self, meta_data):
        if isinstance(meta_data, list):
            for data in meta_data:
                if self.metadata_collection.find_one({'id': data['id']}) is None:
                    self.metadata_collection.insert_one(data)
        elif isinstance(meta_data, dict):
            for key, data in meta_data.items():
                if self.metadata_collection.find_one({'id': key}) is None:
                    self.metadata_collection.insert_one(data)


if __name__ == '__main__':
    import argparse
    from pprint import pprint
    tg_backup = TelegramBackuper('../tg/bin/telegram-cli', '../tg/server.pub')
    parser = argparse.ArgumentParser()
    parser.add_argument('--dial', action='store_true')
    parser.add_argument('--cnt', type=int, default=10)
    parser.add_argument('--off', type=int, default=0)
    parser.add_argument('--hist', action='store_true')
    parser.add_argument('--chat', type=str)
    args = parser.parse_args()
    if args.dial:
        tg_backup.get_gialogs(args.cnt, args.off)
    elif args.hist:
        tg_backup.get_hist_for_id(args.chat)
    else:
        print('Nothing.')
