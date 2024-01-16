import sqlite3
import argparse
import datetime
import time
import os

### from https://apple.stackexchange.com/questions/421665/how-specificially-do-i-read-a-chat-db-file
QUERY = '''
select
 m.rowid
,coalesce(m.cache_roomnames, h.id) ThreadId
,m.is_from_me IsFromMe
,case when m.is_from_me = 1 then m.account
 else h.id end as FromPhoneNumber
,case when m.is_from_me = 0 then m.account
 else coalesce(h2.id, h.id) end as ToPhoneNumber
,m.service Service

/*,datetime(m.date + 978307200, 'unixepoch', 'localtime') as TextDate -- date stored as ticks since 2001-01-01 */
,datetime((m.date / 1000000000) + 978307200, 'unixepoch', 'localtime') as TextDate /* after iOS11 date needs to be / 1000000000 */

,m.text MessageText

,c.display_name RoomName

from
message as m
left join handle as h on m.handle_id = h.rowid
left join chat as c on m.cache_roomnames = c.room_name /* note: chat.room_name is not unique, this may cause one-to-many join */
left join chat_handle_join as ch on c.rowid = ch.chat_id
left join handle as h2 on ch.handle_id = h2.rowid

where
-- try to eliminate duplicates due to non-unique message.cache_roomnames/chat.room_name
(h2.service is null or m.service = h2.service) and
(m.date >= ? and m.date <= ?)

order by m.date asc;
'''

def convert_to_apple_date(date):
	return int(time.mktime(date.timetuple()) - 978307200)*1E9

def main(args):
	if args.end < args.start:
		print("Fatal: End date must be after start date")
		os._exit(-1)
	for file in args.file:
		connection = sqlite3.connect(file)
		cursor = connection.cursor()
		cursor.execute(QUERY, (convert_to_apple_date(args.start), convert_to_apple_date(args.end)))
		for row in cursor.fetchall():
			print(row)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Parse iMessage data")
	parser.add_argument('-f', '--file', nargs='+', default=["~/Library/Messages/chat.db"], help="List of database files")
	parser.add_argument('-o', '--output', help="Output data file")
	parser.add_argument('-s', '--start', help="Start Time", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), required=True)
	parser.add_argument('-e', '--end', help="End Time", type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), required=True)
	parser.add_argument('-n', '--number', nargs='+', default=[], help="Contacts for messages to filter by (omit flag to include all messages)")
	args = parser.parse_args()
	main(args)