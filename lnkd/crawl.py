from bs4 import BeautifulSoup
import config
import cookielib
import fcntl
import os
import random
import re
import smtplib
import sqlite3
import string
import time
import urllib
import urllib2

class LinkedInCrawler(object):

    def __init__(self):
        self.debug = False
        self.crawls_per_run = 100
        self.all_profile_ids = []
        self.cred_queue = []

        # lock and grab top n profiles to crawl
        if not os.path.isfile('.crawl.lck'):
            with open('.crawl.lck', 'a'):
                os.utime('.crawl.lck', None)
        lock_file = open(".crawl.lck","r")
        try:
            fcntl.flock(lock_file.fileno(),fcntl.LOCK_EX)
            if os.path.isfile('queue'):
                os.rename('queue', 'queue.old')
                with open('queue.old', 'r') as f:
                    line_num = 0
                    with open('queue', 'w+') as f2:
                        for line in f.readlines():
                            if line_num < self.crawls_per_run: # grab first n rows to crawl during this run
                                self.cred_queue.append(line.strip())
                            else: # write the rest back to the queue file
                                f2.write(line)
                            self.all_profile_ids.append(int(re.search('id=([0-9]+)', line).group(1)))
                            line_num += 1
                os.remove('queue.old')
        except:
            if os.path.isfile('queue'):
                os.remove('queue')
            if os.path.isfile('queue.old'):
                os.rename('queue.old', 'queue')
            raise
        finally:
            lock_file.close()

        try:
            # SQLite
            self.conn = sqlite3.connect('lnkd.db')
            self.conn.row_factory = sqlite3.Row
            c = self.conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS profile
                         (profile_id real PRIMARY KEY, name text, location text, title text, company text, description text, url text, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_crawled_at DATETIME, updated_at DATETIME)''')
            self.conn.commit()
            c.close()

            # Simulate browser with cookies enabled
            self.cj = cookielib.MozillaCookieJar('parser.cookie.txt')
            self.opener = urllib2.build_opener(
                urllib2.HTTPRedirectHandler(),
                urllib2.HTTPHandler(debuglevel=0),
                urllib2.HTTPSHandler(debuglevel=0),
                urllib2.HTTPCookieProcessor(self.cj)
            )

            self.opener.addheaders = [
                ('User-agent', ('Mozilla/5.0 (Macintosh; '
                               'Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36)'))
            ]

            # login to linkedin
            self.login()

            # crawl queued batch
            if self.debug:
                print '%d profiles have been queued for crawling' % len(self.cred_queue)
            for cred in self.cred_queue[:]:
                with open('queue', 'a+') as f:
                    f.write(cred + '\n')
                self.cred_queue.remove(cred)
                self.crawlProfile(cred)
        except:
            print 'writing pending buffer (%d profiles) to persistent queue' % len(self.cred_queue)
            for cred in self.cred_queue:
                with open('queue', 'a+') as f:
                    f.write(cred + '\n')
            raise
        finally:
            self.conn.close()


    def login(self):
        html = self._loadPage("https://www.linkedin.com/")
        soup = BeautifulSoup(html)
        csrf = soup.find(id="loginCsrfParam-login")['value']
        login_data = urllib.urlencode({
            'session_key': config.LINKEDIN_EMAIL,
            'session_password': config.LINKEDIN_PASSWORD,
            'loginCsrfParam': csrf,
        })
        html = self._loadPage("https://www.linkedin.com/uas/login-submit", login_data)
        soup = BeautifulSoup(html)
        if soup.find(id='verification-code') is not None:
            post_data = {}
            form = soup.find('form')
            for hi in form.select('input[type=hidden]'):
                post_data[hi['name']] = hi['value']
            self._sendMail("LinkedIn is asking for verification code", "you should probably stop your crons")
            code = raw_input("Please enter your verification code: ")
            post_data['PinVerificationForm_pinParam'] = code
            verification_data = urllib.urlencode(post_data)
            html = self._loadPage("https://www.linkedin.com/uas/ato-pin-challenge-submit", verification_data)


    def crawlProfile(self, cred=None):
        url = 'https://www.linkedin.com/profile/view?%s' % cred if cred else 'id=48265010&authType=name&authToken=P42Z'
        if self.debug:
            print 'crawling "%s"' % url
        profile_id = int(re.search('id=([0-9]+)', url).group(1))
        html = self._loadPage(url)
        self._saveProfile(profile_id, url, html)
        soup = BeautifulSoup(html)
        discovery_results = soup.find(class_='discovery-results')
        if len(self.all_profile_ids) <= 10000 and discovery_results is not None:
            similar_profiles = [a['href'] for a in discovery_results.findAll('a') if '/profile/view' in a['href']]
            for profile_url in similar_profiles:
                profile_id = int(re.search('id=([0-9]+)', profile_url).group(1))
                authType = ''
                authToken = ''
                try:
                    authType = re.search('authType=([a-zA-Z_0-9]+)', profile_url).group(1)
                    authToken = re.search('authToken=([a-zA-Z_0-9]+)', profile_url).group(1)
                except:
                    pass
                if profile_id not in self.all_profile_ids:
                    cred = 'id=%s&authType=%s&authToken=%s' % (profile_id, authType, authToken)
                    self.all_profile_ids.append(profile_id)
                    with open('queue', 'a+') as f:
                        f.write(cred + '\n')
                    if self.debug:
                        print 'adding profile to crawl: %s' % cred
        self._delay()

    def _loadPage(self, url, data=None, retry_num=0):
        try:
            if data is not None:
                response = self.opener.open(url, data)
            else:
                response = self.opener.open(url)
            return ''.join(response.readlines())
        except Exception as e:
            if retry_num < 3:
                print 'retrying loadPage...'
                return self._loadPage(url, data, retry_num + 1)
            else:
                print 'Unable to load url "%s" after 3 tries' % url
                raise


    def _saveProfile(self, profile_id, url, html):
        soup = BeautifulSoup(html)
        c = self.conn.cursor()
        profile = {}
        profile['url'] = unicode(url, 'utf-8')
        profile['name'] = soup.select('.full-name')[0].get_text()
        current_positions = soup.select('.background-experience div.current-position')
        if len(current_positions) == 0:
            print 'profile %d - %s: Unable to find any current positions... profile may not be viewable: %s' % (profile_id, profile['name'], url)
        else:
            # parse current position information
            current_position = current_positions[0]
            profile['location'] = soup.select('#location .locality')[0].get_text()
            profile['title'] = current_position.select('header h4')[0].get_text()
            profile['company'] = current_position.select('header h4')[0].nextSibling()[0].get_text()
            description_el = current_position.select('.description')
            if len(description_el) == 0:
                # no description provided for the current position
                profile['description'] = ''
            else:
                profile['description'] = current_position.select('.description')[0].get_text()

                # compare to last run
                c.execute('SELECT * FROM profile WHERE profile_id = ?', (profile_id,))
                saved_profile = c.fetchone()
                description_changed = saved_profile is not None and saved_profile['description'] != profile['description']

                # e-mail if the description changed
                if description_changed:
                    self._sendMail("Job description changed for %s" % profile['name'].encode('utf-8'),
                        'url: %s\n\nold:\n---------\n%s\n\nnew:\n---------\n%s' % (url, saved_profile['description'].encode('utf-8'), profile['description'].encode('utf-8')))

                # save profile to db
                if saved_profile is None or description_changed:
                    c.execute("INSERT OR IGNORE INTO profile (profile_id, name, url) VALUES (?, ?, ?)", (profile_id, profile['name'], url))
                    c.execute("UPDATE profile SET name = ?, url = ?, location = ?, title = ?, company = ?, description = ?, updated_at = CURRENT_TIMESTAMP WHERE profile_id = ?",
                        (profile['name'], url, profile['location'], profile['title'], profile['company'], profile['description'], profile_id,))
                    print 'profile %d - %s: profile %s' % (profile_id, profile['name'], 'updated' if description_changed else 'added')

                c.execute("UPDATE profile SET last_crawled_at = CURRENT_TIMESTAMP WHERE profile_id = ?", (profile_id,))
                self.conn.commit()
        c.close()


    def _sendMail(self, subject, body):
        FROM = "jbwyme@gmail.com"
        TO = ["josh@mixpanel.com"]
        message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (FROM, ",".join(TO), subject, body)
        server = smtplib.SMTP('smtp.gmail.com',587) # port 465 or 587
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(config.GMAIL_EMAIL, config.GMAIL_PASSWORD)
        server.sendmail(FROM, TO, message)
        server.close()


    def _delay(self):
        delay = random.randrange(2,10,1)
        if self.debug:
            print 'waiting %d seconds...' % delay
        time.sleep(delay) # random delay

parser = LinkedInCrawler()
