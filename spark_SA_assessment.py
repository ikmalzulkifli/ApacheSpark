from textblob import TextBlob
from pyspark import SparkConf, SparkContext
import re



def abb_en(line):
   abbreviation_en = {
    'u': 'you',
    'thr': 'there',
    'asap': 'as soon as possible',
    'lv' : 'love',    
    'c' : 'see'
   }
  
   abbrev = ' '.join (abbreviation_en.get(word, word) for word in line.split())
   return (abbrev)

def remove_features(data_str):
   
    url_re = re.compile(r'https?://(\S+)')    
    mention_re = re.compile(r'(@|#)(\w+)')  
    RT_re = re.compile(r'RT(\s+)')
    num_re = re.compile(r'(\d+)')
    
    data_str = str(data_str)
    data_str = data_str.lower() 
    data_str = RT_re.sub('', data_str)   
    data_str = url_re.sub('', data_str)   
    data_str = mention_re.sub('', data_str)  
    data_str = num_re.sub('', data_str)
    return data_str



def polarity(text):
    
    pol = TextBlob(text).sentiment.polarity
    
    if pol > 0.0:
        pol = '+ve'
    elif pol == 0.0:
        pol = 'neu'
    else: pol = '-ve'
        
    return pol
   
  
   
#Write your main function here
def main(sc,filename):
    
    myrawdata = sc.textFile(filename)\
    .map(lambda x: x.split(","))\
    .filter(lambda x:len(x) == 8)\
    .filter(lambda x:len(x[0])>1)
    
    mydata = sc.textFile(filename)\
    .map(lambda x: x.split(","))\
    .filter(lambda x:len(x) == 8)\
    .filter(lambda x:len(x[0])>1)\
    .map(lambda x:x[4])\
    .map(lambda x: x.lower())\
    .map(lambda x:abb_en(x))\
    .map(lambda x:remove_features(x))\
    .map(lambda x:str(x).replace("'","").replace('"',""))\
    .map(lambda x: polarity(x))

    myRDD = mydata.zip(myrawdata).map(lambda x:(x[0]+','+x[1][4]+','+x[1][0]+','+x[1][2]+','+x[1][1]+','+x[1][3]+','+\
    x[1][5]+','+x[1][6]+','+x[1][7])).map(lambda x:str(x).replace('"',"").replace("'",""))
     
                                       
    myRDD.saveAsTextFile("exam1")
   

  
   

if __name__ == "__main__":
    
    conf = SparkConf().setMaster("local[1]").setAppName("exam")
    sc = SparkContext(conf=conf)
    filename ="starbucks_v1.csv"

  
    main(sc,filename)

    sc.stop()
