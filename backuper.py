#encoding:utf-8

import logging
import time
import random

from pytg import Telegram
from pytg.exceptions import IllegalResponseException
from pytg.exceptions import NoResponse

import pymongo
from pymongo.errors import DuplicateKeyError

from tqdm import tqdm


formatter = logging.Formatter(
    '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s'
)
logger = logging.getLogger('TG_BACKUP')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('TG_BACKUP.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


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
        self._RETRY_CNT = 10
        self._delay_constant = 0
        self._fails_in_a_row = 0
        
    def _history(self, chat, count, offset=0, timeout=0):
        wait_for_response = int(10 + timeout)
        try:
            return self.sender.history(chat, count, offset, retry_connect=wait_for_response)
        except IllegalResponseException:
            # No more messages
            return None

    def _dialogs(self, count, offset=0):
        return self.sender.dialog_list(count, offset)

    def _get_retry(self, what, **kwargs):
        retry_n = 0
        while retry_n < self._RETRY_CNT:
            retry_n += 1
            try:
                kwargs['timeout'] = self._delay_constant
                dialogs = getattr(self, '_{w}'.format(w=what))(**kwargs)
                if retry_n == 1:
                    # If everything goes smooth then reset fails_in_a_row counter
                    self._fails_in_a_row = 0
                    # and decrease delay constant
                    self._delay_constant /= 1.01
                    logger.debug('Delay decreased: {d}'.format(d=self._delay_constant))
                return dialogs
            except:
                logger.info('No response from client. Attempt: {n}.'.format(n=retry_n))
                # We should count fails in a row
                if retry_n == 1:
                    self._fails_in_a_row += 1
                # If there is a lot fails, lets increase delay constant
                if self._fails_in_a_row % int(self._RETRY_CNT / 3) == 0:
                    self._delay_constant += 1
                    logger.debug('Delay increased: {d}'.format(d=self._delay_constant))
                if retry_n == self._RETRY_CNT:
                    raise RuntimeError('No response from client.')
                self._sleep_a_little(2 ** retry_n)    

    def get_all_dialogs(self):
        # self.db.drop_collection('metadata')
        # No idea why it gets max 500 contacts.
        bulk_size = 500
        bulks_cnt = 2
        total_dialogs = 0
        well_stored_dialogs = dict()
        for i in range(bulks_cnt):
            offset = i * bulk_size
            dialogs = self._get_retry('dialogs', count=bulk_size, offset=offset)
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
        logger.info(print_name)
        pbar = tqdm(total=bulks * bulk_size, unit='msg', smoothing=0.01)
        meta_data = dict()
        msg_counter = 0
        for i in range(bulks):
            content = list()
            messages = self._get_retry('history', chat=print_name, count=bulk_size, offset=offset)
            if messages is None:
                # There is no more messages
                logger.warning('No more messages.')
                break
            if 'error' in messages:
                raise RuntimeError(messages)
            for msg in messages:
                # pprint(msg)
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
                content_part['from_id'] = msg['from']['id']
                meta_data[msg['from']['id']] = msg['from']
                content_part['to_id'] = msg['to']['id']
                del content_part['from']
                del content_part['to']
                meta_data[msg['to']['id']] = msg['to']
                if 'fwd_from' in content_part:
                    content_part['fwd_from_id'] = msg['fwd_from']['id']
                    meta_data[msg['fwd_from']['id']] = msg['fwd_from']
                # Append finilized content
                content.append(content_part)
                msg_counter += 1
            pbar.update(bulk_size)
            offset += bulk_size
            any_dups = self._store_content(content)
            if any_dups and stop_if_dup:
                break
            self._sleep_a_little()
        self._store_metadata(meta_data)
        pbar.close()
        print('Stored {} messages.'.format(msg_counter))
        print('Stored {} contacts.'.format(len(meta_data)))

    def _sleep_a_little(self, multiply=1):
        time_to_sleep = random.uniform(self._delay_constant, self._delay_constant + 2)
        time.sleep(time_to_sleep * multiply)

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
