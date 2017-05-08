import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager, Link
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
# from lxml import html,etree
import re, os
from time import time
import lxml.html

try:
    # For python 2
    from urlparse import urlparse, parse_qs, urljoin
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set()
    if not os.path.exists("successful_urls.txt") else
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000


@Producer(ProducedLink, Link)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):
    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "52852663_SaiID_36907375"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 UnderGrad {}".format(self.app_id.replace('_', ", "))

        self.frame = frame
        assert (self.UserAgentString != None)
        assert (self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print (l.full_url)
        self.frame.add(l)

    def update(self):
        for g in self.frame.get_new(OneUnProcessedGroup):
            print ("Got a Group")
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)

        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        print ("downloaded ", len(url_count), " in ", time() - self.starttime, " seconds.")
        pass


def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))


def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas


#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''


def extract_next_links(rawDatas):
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    # Loop through UrlResponse objects
    for obj in rawDatas:
        # If object has content, extract links from content
        if obj.content:
            # Convert string to HTML object
            html = lxml.html.fromstring(obj.content)

            # Loop through links in HTML object
            for l in html.iterlinks():
                url = l[2]  # l: (element, attribute, link, pos)

                # break up chained together URLs,
                # sometimes paths looked like this: www.ics.uci.edu//ugrad/policies/Add_Drop_ChangeOption.php/about/QA_Petitions.php/
                # where multiple paths were concatenated together.
                all_urls = re.findall(r'([^:]*?[^:\/]*?\.[^:\/]*?(?:\/|$))',url) # [1:] because ics.uci.edu will be element 0
                if len(all_urls) > 1:
                    if all_urls[0][:2] == '//': all_urls[0] = all_urls[0][2:]; #cleaning up regex problem
                    all_urls = [all_urls[0] + ('/' if all_urls[0][-1] != '/' else '') + p for p in all_urls[1:]] #make not relative
                else:
                    all_urls = [url]
                for r_url in all_urls:
                    abs_url = r_url
                    # If link is not absolute, add host name
                    if not urlparse(abs_url).netloc:  # scheme://netloc/path;parameters?query#fragment

                        # If webpage was redirected, use final_url as host name
                        if obj.is_redirected:
                            host = obj.final_url
                        # Otherwise, just use url as host name
                        else:
                            host = obj.url

                        # Make link absolute
                        abs_url = urljoin(host, abs_url)

                    # Add to output list
                    outputLinks.append(abs_url)

    # Print final result (comment out later)
    # for link in outputLinks:
    #     print(link)

    return outputLinks



already_seen = set()
def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    url = url.lower()

    if 'mailto' in url:
        return False # we can easily ignore mail urls

    if '#' in url:
        url = url[10:url.rfind('#')] # we don't realy care aboute what position to start at in the page

    global already_seen
    if url in already_seen:
        #print 'as'
        return False
    already_seen.add(url)

    parsed = urlparse(url)

    #heuristic: odd urls had concatenated multiple urls together
    fullpath = parsed.path + parsed.query + parsed.params
    if re.search(r'https?://',fullpath):
        #print 'http-in'
        return False

    repetitions = re.finditer(r'(.+?)\1+',fullpath) #get any repeptitions in the string
    for rep in repetitions:
        if len(rep.group(1)) >= 5: #only care about long repeating terms (cant be too small or words like off will trigger...)
            return False
    # heuristic: if there are a lot of parameters it is possibly risky dynamically generated content like calendar.ics.uci.edu
    param_slack = 3
    if len(parse_qs(parsed.query)) >= param_slack:
        #print 'many-params'
        return False

    if parsed.scheme not in set(["http", "https"]):
        #print('no-http')
        return False


    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                            + "|thmx|mso|arff|rtf|jar|csv" \
                            + "|rm|smil|wmv|swf|wma|zip|rar|gz" \
                            + "|lif)$", parsed.path.lower())
    except TypeError:
        print ("TypeError for ", parsed)
