#encoding:utf-8

import logging
import time


from pytg import Telegram
from pytg.exceptions import IllegalResponseException
from pytg.exceptions import NoResponse

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
        self.mongo = pymongo.MongoClient()
        self.db = self.mongo['tg_backup']
        self.content_collection = self.db['content']
        self.metadata_collection = self.db['metadata']
        
    def _history(self, chat, count, offset=0):
        try:
            return self.sender.history(chat, count, offset)
        except IllegalResponseException:
            # No more messages
            return None

    def _gialogs(self, count, offset=0):
        return self.sender.dialog_list(count, offset)

    def get_all_dialogs(self):
        # self.db.drop_collection('metadata')
        # No idea why it gets only 500 contacts.
        bulk_size = 500
        bulks_cnt = 2
        total_dialogs = 0
        well_stored_dialogs = dict()
        for i in range(bulks_cnt):
            offset = i * bulk_size
            dialogs = self._gialogs(bulk_size, offset)
            for dialog in dialogs:
                total_dialogs += 1
                well_stored_dialogs[dialog['id']] = dialog
            self._store_metadata(well_stored_dialogs)
            print('Stored {} dialogs.'.format(total_dialogs))
            print('Gialogs count = {}.'.format(len(well_stored_dialogs)))
            # Better to sleep a little
            time.sleep(5)

    def get_hist_for_id(self, chat_id, bulks=10, offset=0, bulk_size=100, stop_if_dup=False):
        # Get print_name to get history for this name
        chat_id = '${}'.format(chat_id)
        print_name = self.metadata_collection.find_one({'id': chat_id})['print_name']

        pbar = tqdm(total=bulks * bulk_size, unit='msg')
        meta_data = dict()
        msg_counter = 0
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
                msg_counter += 1
            pbar.update(bulk_size)
            offset += bulk_size
            any_dups = self._store_content(content)
            if any_dups and stop_if_dup:
                break
        self._store_metadata(meta_data)
        pbar.close()
        print('Stored {} messages.'.format(msg_counter))
        print('Stored {} contacts.'.format(len(meta_data)))

    def _store_content(self, content):
        # self.content_collection.insert_many(content)
        # Insert many does not work, because of dup key possibility.
        any_dups = False
        for item in content:
            try:
                self.content_collection.insert_one(item)
            except DuplicateKeyError:
                # print('Dup.')
                any_dups = True
        return any_dups

    def _store_metadata(self, meta_data):
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
    parser.add_argument('--cnt', type=int, default=100000)
    parser.add_argument('--off', type=int, default=0)
    parser.add_argument('--bulk_size', type=int, default=100)
    parser.add_argument('--hist', action='store_true',
                        help='Collect some history for given --chat (without $ symbol).')
    parser.add_argument('--chat', type=str)
    parser.add_argument('--stop_if_dup', action='store_true')
    args = parser.parse_args()
    if args.dial:
        tg_backup.get_all_dialogs()
    elif args.hist:
        tg_backup.get_hist_for_id(args.chat, args.cnt, args.off, args.bulk_size, args.stop_if_dup)
    else:
        print('Nothing.')
