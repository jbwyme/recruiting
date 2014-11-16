from bs4 import BeautifulSoup
import config
import cookielib
from decaptcher import Decaptcher
import fcntl
from mixpanel import Mixpanel
import os
import random
import re
import smtplib
import sqlite3
import string
import time
import urllib
import urllib2

CUR_DIR = os.path.dirname(os.path.realpath(__file__))
LOCK_FILE = '%s/%s' % (CUR_DIR, '.crawl.lck')
QUEUE_FILE = '%s/%s' % (CUR_DIR, 'queue')
DB_NAME = 'lnkd.db'

class LinkedInCrawler(object):

    def __init__(self):
        self.debug = True
        self.crawls_per_run = 400
        self.all_profile_ids = []
        self.cred_queue = []
        self.mixpanel = Mixpanel('0abefb20773a18e33f4ded78f5f6613b')
        self.linkedin_username, self.linkedin_password = random.choice(config.LINKEDIN_ACCOUNTS)
        self.started = time.time()
        self._track('Crawl started')

        # lock and grab top n profiles to crawl
        if not os.path.isfile(LOCK_FILE):
            with open(LOCK_FILE, 'a'):
                os.utime(LOCK_FILE, None)
        lock_file = open(LOCK_FILE, 'r')
        try:
            fcntl.flock(lock_file.fileno(),fcntl.LOCK_EX)
            if os.path.isfile(QUEUE_FILE):
                os.rename(QUEUE_FILE, QUEUE_FILE + '.old')
                with open(QUEUE_FILE + '.old', 'r') as f:
                    line_num = 0
                    with open(QUEUE_FILE, 'w+') as f2:
                        for line in f.readlines():
                            if line_num < self.crawls_per_run: # grab first n rows to crawl during this run
                                self.cred_queue.append(line.strip())
                            else: # write the rest back to the queue file
                                f2.write(line)
                            self.all_profile_ids.append(int(re.search('id=([0-9]+)', line).group(1)))
                            line_num += 1
                os.remove(QUEUE_FILE + '.old')
        except:
            if os.path.isfile(QUEUE_FILE):
                os.remove(QUEUE_FILE)
            if os.path.isfile(QUEUE_FILE + '.old'):
                os.rename(QUEUE_FILE + '.old', QUEUE_FILE)
            raise
        finally:
            lock_file.close()

        try:
            # SQLite
            self.conn = sqlite3.connect(CUR_DIR + '/' + DB_NAME)
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
            if self.login():
                self._track('Login success')
                # crawl queued batch
                if self.debug:
                    print '%d profiles have been queued for crawling' % len(self.cred_queue)
                for cred in self.cred_queue[:]:
                    with open(QUEUE_FILE, 'a+') as f:
                        f.write(cred + '\n')
                    self.cred_queue.remove(cred)
                    self.crawlProfile(cred)
            else:
                self._track('Login failure')
                print "unable to login"
        except Exception as e:
            self._track('Exception', {'Profiles in queue': len(self.cred_queue), 'message': str(e)})
            raise
        finally:
            # if there is anything left in the queue, write it out to the queue file
            if len(self.cred_queue) > 0:
                print 'writing pending buffer (%d profiles) to persistent queue' % len(self.cred_queue)
                for cred in self.cred_queue:
                    with open(QUEUE_FILE, 'a+') as f:
                        f.write(cred + '\n')
            self._track('Crawl finished', {'run time (seconds)': int(time.time() - self.started)})
            self.conn.close()


    def login(self):
        if self.debug:
            print "%s, %s" % (self.linkedin_username, self.linkedin_password)
        html = self._loadPage("https://www.linkedin.com/")
        soup = BeautifulSoup(html)
        csrf = soup.find(id="loginCsrfParam-login")['value']
        login_data = urllib.urlencode({
            'session_key': self.linkedin_username,
            'session_password': self.linkedin_password,
            'loginCsrfParam': csrf,
        })
        html = self._loadPage("https://www.linkedin.com/uas/login-submit", login_data)
        soup = BeautifulSoup(html)
        if 'Welcome!' in soup.title.string:
            return True
        if soup.find(id='security-challenge-id-captcha') is not None:
            if self.debug:
                print "Needs captcha solve"
            iframe_html = self._loadPage(soup.find('iframe')['src'])
            iframe_soup = BeautifulSoup(iframe_html)
            image_src = 'https://www.google.com/recaptcha/api/%s' % iframe_soup.find('img')['src']
            urllib.urlretrieve(image_src, 'captcha.jpg')
            d = Decaptcher(config.DECAPTCHER_USERNAME, config.DECAPTCHER_PASSWORD)
            if self.debug:
                print "Solving with decaptcher - balance: %s" % d.get_balance()
            self._track('Captcha presented', {'balance': d.get_balance()})
            solution = d.solve_image('captcha.jpg')
            post_data = {
                'recaptcha_response_field': solution,
            }
            for hi in iframe_soup.find('form').select('input[type=hidden]'):
                post_data[hi['name']] = hi['value']
            captcha_data = urllib.urlencode(post_data)
            captcha_res = self._loadPage(soup.find('iframe')['src'], captcha_data)
            captcha_res_soup = BeautifulSoup(captcha_res)
            code = captcha_res_soup.find('textarea').get_text()
            post_data = {
                'recaptcha_challenge_field': code
            }
            for hi in soup.find('form').select('input[type=hidden]'):
                post_data[hi['name']] = hi['value']
            html = self._loadPage('https://www.linkedin.com/uas/captcha-submit', urllib.urlencode(post_data))
            soup = BeautifulSoup(html)
            if 'Welcome!' in soup.title.string:
                print "Captcha solved!"
                self._track('Captcha solved')
                return True
            else:
                print "Captcha not solved :("
                print soup.title.string
                print html
                self._track('Captcha unsolved')
                return False
        elif soup.find(id='verification-code') is not None:
            post_data = {}
            form = soup.find('form')
            for hi in form.select('input[type=hidden]'):
                post_data[hi['name']] = hi['value']
            self._track('Verification code required')
            self._sendMail("LinkedIn is asking for verification code", "you should probably stop your crons")
            code = raw_input("Please enter your verification code: ")
            post_data['PinVerificationForm_pinParam'] = code
            verification_data = urllib.urlencode(post_data)
            html = self._loadPage("https://www.linkedin.com/uas/ato-pin-challenge-submit", verification_data)
            soup = BeautifulSoup(html)
            if 'Welcome' in soup.title.string:
                return True
            else:
                print "Still not logged in"
                print html
                return False


    def crawlProfile(self, cred=None):
        url = 'https://www.linkedin.com/profile/view?%s' % cred if cred else 'id=48265010&authType=name&authToken=P42Z'
        if self.debug:
            print 'crawling "%s"' % url
        profile_id = int(re.search('id=([0-9]+)', url).group(1))
        self._track('Profile crawled started', {'profile_id': profile_id, 'url': url})
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
                    with open(QUEUE_FILE, 'a+') as f:
                        f.write(cred + '\n')
                    self._track('Profile discovered', {'profile_id': profile_id, 'cred': cred})
                    if self.debug:
                        print 'adding profile to crawl: %s' % cred
        self._delay()

    def _loadPage(self, url, data=None):
        if data is not None:
            response = self.opener.open(url, data)
        else:
            response = self.opener.open(url)
        return ''.join(response.readlines())


    def _saveProfile(self, profile_id, url, html):
        soup = BeautifulSoup(html)
        c = self.conn.cursor()
        profile = {}
        try:
            profile['url'] = unicode(url, 'utf-8')
            name_el = soup.select('.full-name')
            current_positions = soup.select('.background-experience div.current-position')
            if soup.find(id='pagekey-nprofile-out-of-network') is not None:
                print 'profile %d: Out of network' % profile_id
                self._track('Profile inaccessible', {'reason': '"#pagekey-nprofile-out-of-network" found', 'profile_id': profile_id, 'url': url})
            elif len(name_el) == 0:
                print 'profile %d: Unable to get name... probably can\'t access profile but seems to be in network' % profile_id
                print html
                self._track('Profile inaccessible', {'reason': '".full-name" not found', 'profile_id': profile_id, 'url': url})
            elif len(current_positions) == 0:
                print 'profile %d: Unable to find any current positions... profile may not be viewable: %s' % (profile_id, url)
                self._track('Profile inaccessible', {'reason': '".current-positions" not found', 'profile_id': profile_id, 'url': url})
            else:
                # parse current position information
                current_position = current_positions[0]
                profile['name'] = name_el[0].get_text()
                profile['location'] = soup.select('#location .locality')[0].get_text()
                profile['title'] = current_position.select('header h4')[0].get_text()
                profile['company'] = current_position.select('header h4')[0].nextSibling()[0].get_text()
                description_el = current_position.select('.description')
                profile['description'] = '' if len(description_el) == 0 else current_position.select('.description')[0].get_text()

                # compare to last run
                c.execute('SELECT * FROM profile WHERE profile_id = ?', (profile_id,))
                saved_profile = c.fetchone()
                description_changed = saved_profile is not None and saved_profile['description'] != profile['description']

                # e-mail if the description changed
                if description_changed:
                    self._track('Description change found', {'profile_id': profile_id, 'url': url})
                    self._sendMail("Job description changed for %s" % profile['name'].encode('utf-8'),
                        'url: %s\n\nold:\n---------\n%s\n\nnew:\n---------\n%s' % (url, saved_profile['description'].encode('utf-8'), profile['description'].encode('utf-8')))

                # save profile to db
                if saved_profile is None or description_changed:
                    self._track('Profile added', {'profile_id': profile_id, 'url': url})
                    c.execute("INSERT OR IGNORE INTO profile (profile_id, name, url) VALUES (?, ?, ?)", (profile_id, profile['name'], url))
                    c.execute("UPDATE profile SET name = ?, url = ?, location = ?, title = ?, company = ?, description = ?, updated_at = CURRENT_TIMESTAMP WHERE profile_id = ?",
                        (profile['name'], url, profile['location'], profile['title'], profile['company'], profile['description'], profile_id,))
                    print 'profile %d - %s: profile %s' % (profile_id, profile['name'].encode('utf-8'), 'updated' if description_changed else 'added')

                c.execute("UPDATE profile SET last_crawled_at = CURRENT_TIMESTAMP WHERE profile_id = ?", (profile_id,))
                self.conn.commit()
        except IndexError:
            print html
            raise
        finally:
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
        delay = random.randrange(1,10,1)
        if self.debug:
            print 'waiting %d seconds...' % delay
        time.sleep(delay) # random delay

    def _track(self, event, properties={}):
        _props = {
            'LinkedIn Account': self.linkedin_username,
        }
        _props.update(properties)
        self.mixpanel.track(urllib2.urlopen('http://ip.42.pl/raw').read(), event, _props)
try:
    LinkedInCrawler()
except Exception as e:
    print 'CAUGHT THE ERROR'
    raise
