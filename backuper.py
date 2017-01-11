#encoding:utf-8

import logging


from pytg import Telegram
from pytg.exceptions import IllegalResponseException

import pymongo
from pymongo.errors import DuplicateKeyError

from tqdm import tqdm


logger = logging.getLogger('TG_BACKUP')
logger.setLevel(logging.DEBUG)


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
        try:
            return self.sender.history(chat, count, offset)
        except IllegalResponseException:
            # No more messages
            return None

    def _gialogs(self, count, offset=0):
        return self.sender.dialog_list(count, offset)

    def get_gialogs(self, cnt, offset=0):
        self._store_metadata(self._gialogs(cnt, offset))

    def prepare_content(self, msg):
        pass

    def get_hist_for_id(self, chat_id, bulks=10, offset=0, bulk_size=100):
        # Get print_name to get history for this name
        chat_id = '${}'.format(chat_id)
        print_name = self.metadata_collection.find_one({'id': chat_id})['print_name']

        pbar = tqdm(total=bulks * bulk_size, unit='msg')
        meta_data = dict()
        for i in range(bulks):
            content = list()
            messages = self._history(print_name, bulk_size, offset)
            if messages is None:
                # There is no more messages
                logger.warning('No more messages.')
                break
            for msg in messages:
                content_part = dict(msg.copy())
                content_part['_id'] = content_part['id']
                # Remove useless stuff
                del content_part['id']
                del content_part['unread']
                del content_part['out']
                del content_part['flags']
                # Add chat_id, it will be easy to search
                content_part['chat_id'] = chat_id
                # Remove duplicated metadata
                content_part['from'] = {'id': msg['from']['id']}
                meta_data[msg['from']['id']] = msg['from']
                content_part['to'] = {'id': msg['to']['id']}
                meta_data[msg['to']['id']] = msg['to']
                if 'fwd_from' in content_part:
                    content_part['fwd_from'] = {'id': msg['fwd_from']['id']}
                    meta_data[msg['fwd_from']['id']] = msg['fwd_from']
                # Append finilized content
                content.append(content_part)
            pbar.update(bulk_size)
            offset += bulk_size
            self._store_content(content)
        self._store_metadata(meta_data)
        pbar.close()

    def _store_content(self, content):
        # self.content_collection.insert_many(content)
        # Insert many does not work, because of dup key possibility.
        for item in content:
            try:
                self.content_collection.insert_one(item)
            except DuplicateKeyError:
                # print('Dup.')
                pass

    def _store_metadata(self, meta_data):
        if isinstance(meta_data, list):
            # from dialogs
            for data in meta_data:
                if self.metadata_collection.find_one({'id': data['id']}) is None:
                    self.metadata_collection.insert_one(data)
        elif isinstance(meta_data, dict):
            # from content
            for key, data in meta_data.items():
                if self.metadata_collection.find_one({'id': key}) is None:
                    self.metadata_collection.insert_one(data)


if __name__ == '__main__':
    import argparse
    from pprint import pprint
    tg_backup = TelegramBackuper('../tg/bin/telegram-cli', '../tg/server.pub')
    parser = argparse.ArgumentParser(description='Get some Telegram history.')
    parser.add_argument('--dial', action='store_true',
                        help='Collect some dialogs metadata.')
    parser.add_argument('--cnt', type=int, default=10)
    parser.add_argument('--off', type=int, default=0)
    parser.add_argument('--bulk_size', type=int, default=100)
    parser.add_argument('--hist', action='store_true',
                        help='Collect some history for given --chat (without $ symbol).')
    parser.add_argument('--chat', type=str)
    args = parser.parse_args()
    if args.dial:
        tg_backup.get_gialogs(args.cnt, args.off)
    elif args.hist:
        tg_backup.get_hist_for_id(args.chat, args.cnt, args.off, args.bulk_size)
    else:
        print('Nothing.')
