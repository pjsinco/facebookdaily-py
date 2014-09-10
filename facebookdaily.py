#!/usr/bin/env python

"""
This program updates our facebook database to include the latest values for
likes, shares, comments, etc.
"""
import MySQLdb as mysql
import urllib2
import urllib
import datetime
from xml.dom.minidom import parseString
import settings
import sys
from pprint import pprint
from HTMLParser import HTMLParser

def main():
  print "Facebook report for %s\n" % \
    datetime.datetime.now().strftime('%A, %B %-d, %Y')
  print "Activity since yesterday ...\n"

  # get all id's of published posts
  postList = getAllInfoForAllPosts()

  has_output = False

  # check each one to see if the db needs to be updated to reflect new
  # facebook likes, counts, shares, etc.
  for post in postList: 
    id = post[0]
    #print "Checking %s" % (post[1])
    updatedVal = getUpdatedVal(post)

    # if we have no entry at all for this it, add a db entry
    if not isInDb(id):
      insertEntry(updatedVal['id'], updatedVal, {}, True)
      has_output = True
      continue # skip the rest of this loop iteration
    else:
      currentVal = getCurrentVal(id)

    # if values have changed, update the db
    if valsAreEqual(updatedVal, currentVal) == False:
      insertEntry(updatedVal['id'], updatedVal, currentVal, False)
      has_output = True

  if not has_output:
    print("No changes")


def permalink(slug, date):
  """ Returns a link in the form of 
      http://thedo.osteopathic.org/2014/09/greys-anatomy-vs-real-life-residency/
  """
  return '%s/%d/%s/%s/' % (settings.BASE_URL, date.year, str(date.month).zfill(2), slug)

def isInDb(id):
  """ Returns a boolean indicating whether there's already
      an entry in the db for the story
  """
  db = mysql.connect(settings.HOST, settings.USER, settings.PW, \
    settings.DB, charset='utf8', use_unicode=True)
  cur = db.cursor()
  query = "SELECT * FROM %s WHERE id = %s" % (settings.FB_TABLE, str(id))
  try:
    cur.execute(query)
    if cur.fetchone() == None:
      return False
  except mysql.Error:
    print "Error querying the database"
  finally:
    cur.close()
    db.close()
  return True
  

def getAllInfoForAllPosts():
  """ Returns a list of lists(id, slug, date) of all published stories """
  # set up database connection
  # IMPORTANT: set the charset and use_unicode args
  db = mysql.connect(
    settings.HOST, settings.USER, settings.PW,
    settings.DB, charset='utf8', use_unicode=True
  )
  cur = db.cursor()
  try: 
    cur.execute(settings.QUERY_GET_POST_INFO)
    results = cur.fetchall()
    #list = [record[0] for record in results]    # list comprehension!
    rows = []
    for record in results:
      row = []
      for field in record:
        row.append(field)
      rows.append(row)
  except mysql.Error:
    print "Error querying the database"
  finally:
    cur.close()
    db.close()
  return rows
  
def getUpdatedVal(post):
  """ Return a dict of the most current Facebook counts.
      Accepts a list of 3 items: id, slug, date
  """
  if (type(post) is list and len(post) == 3):
    id = post[0]
    slug = post[1]
    date = post[2]

  updated = dict.fromkeys(settings.FB_KEYS)
  updated['id'] = id
  updated['date'] = str(datetime.date.today())

  # form the url
  url = settings.FB_URL + urllib.quote_plus(permalink(slug, date))
  
  # query facebook,read the xml and close the source file
  file = urllib2.urlopen(url)
  xmldoc = file.read()
  file.close()
  
  # build the dom
  dom = parseString(xmldoc)
  
  # pull values from the xml and add to updated dict
  updated['url'] = \
    dom.getElementsByTagName('url')[0].firstChild.data
  updated['shares'] = \
    dom.getElementsByTagName('share_count')[0].firstChild.data
  updated['likes'] = \
    dom.getElementsByTagName('like_count')[0].firstChild.data
  updated['comments'] = \
    dom.getElementsByTagName('comment_count')[0].firstChild.data
  updated['clicks'] = \
    dom.getElementsByTagName('click_count')[0].firstChild.data
  try:
    updated['fbid'] = \
      dom.getElementsByTagName('comments_fbid')[0].firstChild.data
  except:
    pass
  return updated

def getCurrentVal(id):
  """ Return a dict of Facebook counts in the database"""
  current = dict.fromkeys(settings.FB_KEYS)
  db = mysql.connect(settings.HOST, settings.USER, settings.PW, \
    settings.DB, charset='utf8', use_unicode=True)
  cur = db.cursor()
  
  query = "SELECT * FROM facebook WHERE id = %s and date = " \
    "(SELECT max(date) FROM facebook WHERE id = %s)" % (id, id)
  try:
    cur.execute(query)
    results = cur.fetchone()
    #assert(len(results) == 8)
    current['id'] = results[0]
    current['url'] = results[1]
    current['shares'] = results[2]
    current['likes'] = results[3]
    current['comments'] = results[4]
    current['clicks'] = results[5]
    current['fbid'] = results[6]
    current['date'] = results[7]
  except mysql.Error:
    print "Error querying the database"
  finally:
    cur.close()
    db.close()
  return current
  
def getHeadline(id):
  """ Return the headline of the post with the given id """
  # IMPORTANT: set the charset and use_unicode args
  
  db = mysql.connect(settings.HOST, settings.USER, settings.PW, \
  settings.DB, charset='utf8', use_unicode=True)
  cur = db.cursor()
  query = """
    SELECT post_title
    FROM wp_posts
    WHERE id = %s
  """ % (str(id))
  try: 
    cur.execute(query)
    results = cur.fetchone()[0]
  except mysql.Error:
    print "Error querying the database"
  finally:
    cur.close()
    db.close()

  return HTMLParser().unescape(results)

def valsAreEqual(updated, current):
  """ Compares updated and current returns false if they're different.
  """

  if int(updated['shares']) != int(current['shares']):
    #print 'shares is off'
    return False
  elif int(updated['id']) != int(current['id']):
    #print 'id is off'
    return False
  elif int(updated['clicks']) != int(current['clicks']):
    #print 'clicks is off'
    return False
  elif int(updated['likes']) != int(current['likes']):
    #print 'likes is off'
    return False
  elif int(updated['comments']) != int(current['comments']):
    #print 'comments is off'
    return False
  else:
    return True

def insertEntry(id, updatedVal, currentVal, new = False):
  """
  Inserts a entry into the database
  id: the id of hte article to insert
  updatedVal: the dict of updated values
  new: True if the entry is new, false otherwise
  """
  if new == True:
    print "New entry:\n"
    print "Inserting %s\n"\
      "Shares:%5s\n"\
      "Likes:%6s\n" \
      "Comments:%3s" % (
      getHeadline(id), 
      updatedVal['shares'],
      updatedVal['likes'],  
      updatedVal['comments']
      )
  else:
    print """
------------------
 U  P  D  A  T  E
------------------
    """
    print "%s\n"\
      "New shares:%5d\n"\
      "New likes:%6d\n"\
      "New comments:%3d\n\n"\
      "N E W   T O T A L S\n"\
      "Shares:%9d\n"\
      "Likes:%10d\n"\
      "Comments:%7d\n\n"\
      % (\
        getHeadline(id),\
        int(updatedVal['shares']) - int(currentVal['shares']),\
        int(updatedVal['likes']) - int(currentVal['likes']),\
        int(updatedVal['comments']) - int(currentVal['comments']),\
        int(updatedVal['shares']),\
        int(updatedVal['likes']),\
        int(updatedVal['comments'])\
      )

  # open db connection
  db = mysql.connect(settings.HOST, settings.USER, settings.PW, \
    settings.DB, charset='utf8', use_unicode=True)
  cur = db.cursor()
  
  # prep query
  query = """
  INSERT INTO facebook (id, url, share_count, like_count, 
  comment_count, click_count, comments_fbid, date) 
  VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')
  """ % (updatedVal['id'], updatedVal['url'], updatedVal['shares'], \
    updatedVal['likes'], updatedVal['comments'], updatedVal['clicks'], \
    updatedVal['fbid'], updatedVal['date'])

  # try the insertion
  try:
    cur.execute(query)
    db.commit()       # commit changes
    #print "Operation successful!"
  except mysql.Error:
    db.rollback()   # roll back in case of an error
    print "Operation failed. A database error occurred. Insertion rolled back."
  finally:
    cur.close()
    db.close()

# --------------------------------------------- #
if __name__ == '__main__':
  main()
