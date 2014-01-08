#!/usr/bin/env python

"""
This program updates our facebook database to include the latest values for
likes, shares, comments, etc.
"""
import MySQLdb as mysql
import urllib2
import datetime
from xml.dom.minidom import parseString
import settings

def main():
  print "Checking Facebook ..."

  # get all id's of published posts
  idList = getListOfIds()

  # check each one to see if the db needs to be updated to reflect new
  # facebook likes, counts, shares, etc.
  for id in idList: 
    print "Checking %s" % (id)
    updatedVal = getUpdatedVal(id)
    currentVal = getCurrentVal(id)

    # if we have no entry at all for this it, add a db entry
    if isInDb(id) == False:
      insertEntry(updatedVal['id'], updatedVal, True)
    
    # if values have changed, update the db
    if valsAreEqual(updatedVal, currentVal) == False:
      insertEntry(updatedVal['id'], updatedVal, False)

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
  db.close()
  return True
  

def getListOfIds():
  """ Returns a list of id's of all published stories """
  # set up database connection
  # IMPORTANT: set the charset and use_unicode args
  db = mysql.connect(settings.HOST, settings.USER, settings.PW, \
    settings.DB, charset='utf8', use_unicode=True)
  cur = db.cursor()
  try: 
    cur.execute(settings.QUERY_GET_POST_IDS)
    results = cur.fetchall()
    list = [record[0] for record in results]    # list comprehension!
  except mysql.Error:
    print "Error querying the database"
  return list
  
def getUpdatedVal(id):
  """ Return a dict of the most current Facebook counts """
  updated = dict.fromkeys(settings.FB_KEYS)
  updated['id'] = id
  updated['date'] = datetime.date.today()

  # form the url
  url = settings.FB_URL + str(id)
  
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
  updated['fbid'] = \
    dom.getElementsByTagName('comments_fbid')[0].firstChild.data
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
    assert(len(results) == 8)
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
  db.close()
  return results

def valsAreEqual(updated, current):
  """ Compares updated and current returns false if they're different.
  """

  if int(updated['shares']) != int(current['shares']):
    print 'shares is off'
    return False
  elif int(updated['id']) != int(current['id']):
    print 'id is off'
    return False
  elif int(updated['clicks']) != int(current['clicks']):
    print 'clicks is off'
    return False
  elif int(updated['likes']) != int(current['likes']):
    print 'likes is off'
    return False
  elif int(updated['comments']) != int(current['comments']):
    print 'comments is off'
    return False
  else:
    return True

def insertEntry(id, updatedVal, new = False):
  """
  Inserts a entry into the database
  id: the id of hte article to insert
  updatedVal: the dict of updated values
  new: True if the entry is new, false otherwise
  """
  if new == True:
    print "New entry!"
    print "Inserting %s\n%s\nShares:%5s\nLikes:%6s\n" \
      "Comments:%3s" % (updatedVal['id'], getHeadline(id), updatedVal['shares'], \
      updatedVal['likes'],  updatedVal['comments'])
  else:
    print "Updating %s\n%s\nShares:%5s\nLikes:%6s\n" \
      "Comments:%3s" % (updatedVal['id'], getHeadline(id), updatedVal['shares'], \
      updatedVal['likes'], updatedVal['comments'])

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
    print "Operation successful!"
  except mysql.Error:
    db.rollback()   # roll back in case of an error
    print "Operation failed. A database error occurred. Insertion rolled back."
  db.close()

# --------------------------------------------- #
if __name__ == '__main__':
  main()