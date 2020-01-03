# -*- coding: utf-8 -*-
"""
Created on Sun Dec 15 20:43:29 2019

@author: jcopelan
"""
import pandas as pd
import numpy as np
import io, requests, os, time, sys, locale, getpass, time, json

clientId = 'c6uyt0v166cnxsb62ww9kctbebhiwc7y.i'
clientSecret = 'nmlye5ge6bynqgk38itfqntwdyqgubju.s'

class Community:
    """ a wrapper class for the Jive Community Rest API"""
    
    def __init__(self, clientId, clientSecret):
        """Instantiane the class with parameters for clientId and clientSecret"""
        self.clientId = clientId
        self.clientSecret = clientSecret
    
    def callAPI(self, refresh_time, key, next_url):

        ids = {'clientId': self.clientId, 'clientSecret': self.clientSecret}

    	#Tries request 5 times or until HTTP 200 is returned
        tries = 5
        for i in range(tries):
            try:
    			#refreshes authorization key every 20 minutes
                if time.time() >= refresh_time:
                    key = requests.post("https://api.jivesoftware.com/analytics/v1/auth/login", params=ids)
                    refresh_time = time.time() + 1200
    
    			#Make API request using authorization token
                headers = {'Authorization': key.text}
                r = requests.get(next_url, headers=headers)
    
            except requests.exceptions.Timeout:
                print("Timeout error, #", i+1, " try of ", tries)
            except requests.exceptions.TooManyRedirects:
                print("Redirect Error, #", i+1, " try of", tries)
            except requests.exceptions.RequestException as e:
                print("RequestException: #", i+1, " try of ", tries, e)
                if i == (tries - 1):
                    print("Script must be re-run, RequestException: ", e)
                    sys.exit(1)
                else:
    			#break from re-try loop when HTTP 200 is received.  Refresh auth token in case of HTTP 401.
                    if r.status_code == 200:
                        break
                    elif r.status_code == 401:
                        refresh_time = 0
                    else:
                        print('Bad status returned from API, #', i+1, 'try of ', tries, '. HTTP', r.status_code)
                              
            return refresh_time, key, r

    def getQuestions(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=match(activity.actionObject.objectType,question)&filter=action(Create,Update)'
        next_url = url + filters
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                newd = {'objectId': [data['list'][dataelement]['actionObjectId']],
                        'type': [data['list'][dataelement]['name']],
                        'containerId': [data['list'][dataelement]['containerId']],
                        'containerType': [data['list'][dataelement]['containerType']],
                        'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                        #'reply': [data['list'][dataelement]['activity']['actionObject']['extras']['isRootReply']],
                        #'threadId': [data['list'][dataelement]['activity']['actionObject']['extras']['forumThreadId']],
                        #'messageParentId': [data['list'][dataelement]['activity']['actionObject']['extras']['messageParentId']],
                        'subject': [data['list'][dataelement]['activity']['actionObject']['subject']],
                        'username': [data['list'][dataelement]['activity']['actionObject']['author']['username']],
                        'containerName': [data['list'][dataelement]['activity']['destination']['name']]
                        }
                newdf = pd.DataFrame(data=newd)
                datafile.append(newdf)
        
            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        return dataresult

    def deletedQuestions(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=match(activity.actionObject.objectType,question)&filter=action(Delete)'
        next_url = url + filters
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                newd = {'objectId': [data['list'][dataelement]['actionObjectId']],
                        'type': [data['list'][dataelement]['name']],
                        'containerId': [data['list'][dataelement]['containerId']],
                        'containerType': [data['list'][dataelement]['containerType']],
                        'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                        #'reply': [data['list'][dataelement]['activity']['actionObject']['extras']['isRootReply']],
                        #'threadId': [data['list'][dataelement]['activity']['actionObject']['extras']['forumThreadId']],
                        #'messageParentId': [data['list'][dataelement]['activity']['actionObject']['extras']['messageParentId']],
                        'subject': [data['list'][dataelement]['activity']['actionObject']['subject']],
                        'username': [data['list'][dataelement]['activity']['actionObject']['author']['username']],
                        'containerName': [data['list'][dataelement]['activity']['destination']['name']]
                        }
                newdf = pd.DataFrame(data=newd)
                datafile.append(newdf)
        
            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        return dataresult
    
    def getThreads(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=match(activity.actionObject.objectType,thread)&filter=action(Create,Update)'
        next_url = url + filters
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                newd = {'objectId': [data['list'][dataelement]['actionObjectId']],
                        'type': [data['list'][dataelement]['name']],
                        'containerId': [data['list'][dataelement]['containerId']],
                        'containerType': [data['list'][dataelement]['containerType']],
                        'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                        #'reply': [data['list'][dataelement]['activity']['actionObject']['extras']['isRootReply']],
                        #'threadId': [data['list'][dataelement]['activity']['actionObject']['extras']['forumThreadId']],
                        #'messageParentId': [data['list'][dataelement]['activity']['actionObject']['extras']['messageParentId']],
                        'subject': [data['list'][dataelement]['activity']['actionObject']['subject']],
                        'username': [data['list'][dataelement]['activity']['actionObject']['author']['username']],
                        'containerName': [data['list'][dataelement]['activity']['destination']['name']]
                        }
                newdf = pd.DataFrame(data=newd)
                datafile.append(newdf)
        
            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        return dataresult
    
    def deletedThreads(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=match(activity.actionObject.objectType,thread)&filter=action(Delete)'
        next_url = url + filters
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                newd = {'objectId': [data['list'][dataelement]['actionObjectId']],
                        'type': [data['list'][dataelement]['name']],
                        'containerId': [data['list'][dataelement]['containerId']],
                        'containerType': [data['list'][dataelement]['containerType']],
                        'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                        'containerName': [data['list'][dataelement]['activity']['destination']['name']]
                        }
                newdf = pd.DataFrame(data=newd)
                datafile.append(newdf)
        
            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        return dataresult
    
    def getThreadViews(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=match(activity.actionObject.objectType,thread)&filter=action(View)'
        fields = '&fields=actionObjectId,name,containerId,containerType,timestamp,activity.actor.username,' \
                + 'activity.destination.name,activity.actionObject.questionStatus,activity.actionObject.resolutionDate,activity.actionObject.numReplies,' \
                + 'activity.actionObject.numHelpfulAnswers'
        request_size = '&count=100000'
        next_url = url + filters + fields + request_size
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                try:
                    newd = {'objectId': [data['list'][dataelement]['actionObjectId']],
                            'type': [data['list'][dataelement]['name']],
                            'containerId': [data['list'][dataelement]['containerId']],
                            'containerType': [data['list'][dataelement]['containerType']],
                            'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                            'username': [data['list'][dataelement]['activity']['actor']['username']],
                            'containerName': [data['list'][dataelement]['activity']['destination']['name']],
                            'questionStatus': [data['list'][dataelement]['activity']['actionObject']['questionStatus']],
                            'resolutionDate': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['activity']['actionObject']['resolutionDate'] / 1000.))],
                            'numberReplies': [data['list'][dataelement]['activity']['actionObject']['numReplies']],
                            'numberHelpful': [data['list'][dataelement]['activity']['actionObject']['numHelpfulAnswers']],
                            'count': [1]}
                    newdf = pd.DataFrame(data=newd)
                    datafile.append(newdf)
                except KeyError:
                    pass
                
        
            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        return dataresult
    
    def getAllViews(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=action(View)'
        fields = '&fields=seqId,timestamp,actionObjectId,actionObjectType,activity.actor.username,activity.actionObject.objectType'
        request_size = '&count=100000'
        next_url = url + filters + fields + request_size
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                try:
                    newd = {'viewIndex': [data['list'][dataelement]['seqId']],
                            'objectId': [data['list'][dataelement]['actionObjectId']],
                            'objectTypeCode': [data['list'][dataelement]['actionObjectType']],
                            'objectTypeDescription': [data['list'][dataelement]['activity']['actionObject']['objectType']],
                            'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                            'username': [data['list'][dataelement]['activity']['actor']['username']],
                            'count': [1]}
                    newdf = pd.DataFrame(data=newd)
                    datafile.append(newdf)
                except:
                    pass

            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        return dataresult
    
    def updateViews(self, filename):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity/lastweek?'
        filters = 'filter=action(View)'
        fields = '&fields=seqId,timestamp,actionObjectId,actionObjectType,activity.actor.username,activity.actionObject.objectType'
        request_size = '&count=100000'
        next_url = url + filters + fields + request_size
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                try:
                    newd = {'viewIndex': [data['list'][dataelement]['seqId']],
                            'objectId': [data['list'][dataelement]['actionObjectId']],
                            'objectTypeCode': [data['list'][dataelement]['actionObjectType']],
                            'objectTypeDescription': [data['list'][dataelement]['activity']['actionObject']['objectType']],
                            'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                            'username': [data['list'][dataelement]['activity']['actor']['username']],
                            'count': [1]}
                    newdf = pd.DataFrame(data=newd)
                    datafile.append(newdf)
                except:
                    pass

            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        dataresult['dataIndex'] = dataresult['viewIndex'].astype(str) + dataresult['objectId'].astype(str) + dataresult['timestamp'].astype(str) + dataresult['objectTypeCode'].astype(str)
        
        try:
            olddata = pd.DataFrame.from_csv(filename, encoding = 'ISO-8859-1')
            olddata['dataIndex'] = olddata['viewIndex'].astype(str) + olddata['objectId'].astype(str) + olddata['timestamp'].astype(str) + olddata['objectTypeCode'].astype(str)
        except FileNotFoundError:
            print('That file doesnt exist. Script Exiting')
            
        #set indexes in both files for upserting purposes
        olddata.set_index('dataIndex', inplace = True)
        dataresult.set_index('dataIndex', inplace = True)
        
        updateddata = pd.concat([olddata, dataresult[~dataresult.index.isin(olddata.index)]])
        updateddata['type'] = 'View'
        updateddata.to_csv(filename)   
        
        return updateddata
    
    def getLikes(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=action(Like,Unlike)'
        fields = '&fields=name,activity.actionObject.extras.acclaimId,timestamp,actionObjectId,actionObjectType,activity.actor.username,activity.actionObject.objectType'
        request_size = '&count=100000'
        next_url = url + filters + fields + request_size
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                try:
                    newd = {'viewIndex': [data['list'][dataelement]['activity']['actionObject']['extras']['acclaimId']],
                            'objectId': [data['list'][dataelement]['actionObjectId']],
                            'objectTypeCode': [data['list'][dataelement]['actionObjectType']],
                            'objectTypeDescription': [data['list'][dataelement]['activity']['actionObject']['objectType']],
                            'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                            'username': [data['list'][dataelement]['activity']['actor']['username']],
                            'actionType': [data['list'][dataelement]['name']]
                            }
                    newdf = pd.DataFrame(data=newd)
                    datafile.append(newdf)
                except:
                    pass

            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        dataresult['count'] = dataresult['actionType'].apply(lambda x: np.where(x[0:13] == 'ACTIVITY_LIKE', 1, -1))
        dataresult['type'] = 'Like'
        
        dataresult.drop(labels = 'actionType', inplace = True, axis = 1)
        
        return dataresult
    
    def getBookmarks(self):
        url = 'https://api.jivesoftware.com/analytics/v2/export/activity?'
        filters = 'filter=action(Bookmark,Unbookmark)'
        fields = '&fields=name,activity.actionObject.extras.bookmarkAuthors,timestamp,actionObjectId,actionObjectType,activity.actor.username,activity.actionObject.objectType'
        request_size = '&count=100000'
        next_url = url + filters + fields + request_size
        total_pages = ""
        current_page = ""
        refresh_time = 0
        key = ""
        
        datafile = []
        condition = True
        while(condition):
            refresh_time, key, r = self.callAPI(refresh_time, key, next_url)
            if r.status_code == 200:
                data = json.loads(r.text)
            else:
                print('Bad status returned from API.  HTTP', r.status_code)
                break
            
            for dataelement in range(len(data['list'])):
                try:
                    newd = {'viewIndex': [data['list'][dataelement]['activity']['actionObject']['extras']['bookmarkAuthors']],
                            'objectId': [data['list'][dataelement]['actionObjectId']],
                            'objectTypeCode': [data['list'][dataelement]['actionObjectType']],
                            'objectTypeDescription': [data['list'][dataelement]['activity']['actionObject']['objectType']],
                            'timestamp': [time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(data['list'][dataelement]['timestamp'] / 1000.))],
                            'username': [data['list'][dataelement]['activity']['actor']['username']],
                            'actionType': [data['list'][dataelement]['name']]
                            }
                    newdf = pd.DataFrame(data=newd)
                    datafile.append(newdf)
                except:
                    pass

            current_page = data['paging']['currentPage']
            total_pages = data['paging']['totalPages']
            
            if (total_pages != current_page):
                next_url = data['paging']['next']
                
            print("Wrote page ", current_page, " of ", total_pages, " at ", time.strftime("%H:%M:%S, %Y/%m/%d", time.localtime(time.time())))
    
            if (total_pages == current_page) or (int(total_pages) == 0):
                condition = False
    
        dataresult = pd.concat(datafile, axis=0)
        dataresult['count'] = dataresult['actionType'].apply(lambda x: np.where(x[0:17] == 'ACTIVITY_BOOKMARK', 1, -1))
        dataresult['type'] = 'Bookmark'
        
        dataresult.drop(labels = 'actionType', inplace = True, axis = 1)
        
        return dataresult