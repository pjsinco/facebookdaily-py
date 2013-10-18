import MySQLdb as mysql
import urllib2
import datetime
from xml.dom.minidom import parseString

FB_URL = ("http://api.facebook.com/restserver.php?method=links."
  "getStats&urls=http%3A%2F%2Fwww.do-online.org%2FTheDO%2F%3Fp%3D")
QUERY_GET_POST_IDS = """
    SELECT w.id
    FROM wp_posts w 
    WHERE w.post_status = 'publish'
      AND w.post_type = 'post'
    ORDER BY w.id
  """
FB_TABLE = "facebook"

config = open('config.dat', 'r')

HOST = config.readline().rstrip('\n')
USER = config.readline().rstrip('\n')
PASSWORD = config.readline().rstrip('\n')
DB = config.readline().rstrip('\n')
# print 'host: %s\nuser: %s\npw: %s\ndb: %s\n' % (HOST, USER, PASSWORD, DB)

config.close()

def getHeadline(id):
  db = mysql.connect(HOST, USER, PASSWORD, DB)
  cur = db.cursor()
  query = """
    SELECT post_title
    FROM wp_posts
    WHERE id = %s
  """ % (str(id))
  cur.execute(query)
  return cur.fetchone()[0]


db = mysql.connect(HOST, USER, PASSWORD, DB)
cur = db.cursor()
cur.execute(QUERY_GET_POST_IDS)

# turn our results into a list because fetchall() returns a tuple
results = cur.fetchall()
idList = [record[0] for record in results]

updatedVal = [] 

# add today's date to index 7
updatedVal.insert(7, datetime.date.today())

for id in idList: 
  url = FB_URL + str(id)

  # query the facebook page
  file = urllib2.urlopen(url)

  # read the xml and close the source file
  xmldoc = file.read()
  file.close()

  # build the dom
  dom = parseString(xmldoc)

  # pull values from the xml and add to updatedVal list
  fbUrl = dom.getElementsByTagName('url')[0].firstChild.data
  updatedVal.insert(1, fbUrl)
  fbShares = dom.getElementsByTagName('share_count')[0].firstChild.data
  updatedVal.insert(2, fbShares)
  fbLikes = dom.getElementsByTagName('like_count')[0].firstChild.data
  updatedVal.insert(3, fbLikes)
  fbComments = dom.getElementsByTagName('comment_count')[0].firstChild.data
  updatedVal.insert(4, fbComments)
  fbClicks = dom.getElementsByTagName('click_count')[0].firstChild.data
  updatedVal.insert(5, fbClicks)
  fbCommentsFbid = dom.getElementsByTagName('comments_fbid')[0].firstChild.data
  updatedVal.insert(6, fbCommentsFbid)
  
  # if there's no already an entry for this id in the db, add it
  # 
  ind = idList.index(id)
  query = "SELECT * FROM %s WHERE id = %s" % (FB_TABLE, str(id))
  cur.execute(query)
  if cur.fetchone() <> None:
    print getHeadline(id)
    print ("Inserting %s\n%s\nShares:%5s\nLikes:%6s\n"
            "Comments:%3s\n" % (str(id), getHeadline(id), fbShares, fbLikes,
            fbComments))
