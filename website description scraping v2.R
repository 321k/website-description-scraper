# Load required packages
library(RMySQL)
library(dplyr)
library(rvest)
library(RCurl)
library(XML)

?getURIAsynchronous

# Close open connections
all_cons <- dbListConnections(MySQL())
for(con in all_cons) dbDisconnect(con)

# Connect to db via ssh ssh stats@looker.transferwise.com -L 3307:172.18.1.43:3306
con <- dbConnect(MySQL(),
                 user = .env$usr,
                 password = .env$pw,
                 host = '127.0.0.1',
                 port = 3307,
                 dbname='obfuscated')

# Define functinos
htmlParse_error = function(x){
  result = tryCatch({
    htmlParse(x, asText=TRUE, encoding = "UTF-8")
  }, warning = function(w) {
    htmlParse(x, asText=TRUE, encoding = "UTF-8")
  }, error = function(e) {
    ""
  }, finally = {
  })
  return(result)
}

xpathSApply_error = function(x, y){
  result = tryCatch({
    xpathSApply(x, y)
  }, warning = function(w) {
    xpathSApply(x, y)
  }, error = function(e) {
    ""
  }, finally = {
  })
  return(result)
}
x = users[i:(i+step_size-1),]
get_descriptions = function(x){
  urls = x$webpage
  user_ids = x$user_id
  source_content <- getURIAsynchronous(urls)
  html_content = sapply(source_content, function(x) htmlParse_error(x))
  rm = which(html_content == "")
  if(length(rm)>0){
    html_content = html_content[-rm]
    user_ids = user_ids[-rm]
  } 
  length(html_content)
  length(user_ids)
  
  description = lapply(html_content, function(x) xpathSApply_error(x, "//meta[@name='description']/@content"))
  names(description) = 1:length(description)
  lapply(description, class)
  description = unlist(description)
  
  length(description)
  length(user_ids)
  
  include = grep('content', names(description))
  description[include] %>% length
  user_ids[include] %>% length
  description = description[include]
  user_ids = user_ids[include]
  
  result = data.frame(user_id = user_ids, website_description = description)
  return(result)
}

# Select new businesses with a website

# Close open connections
all_cons <- dbListConnections(MySQL())
for(con in all_cons) dbDisconnect(con)

# Connect to db via ssh ssh stats@looker.transferwise.com -L 3307:172.18.1.43:3306
con <- dbConnect(MySQL(),
                 user = .env$usr,
                 password = .env$pw,
                 host = '127.0.0.1',
                 port = 3307,
                 dbname='obfuscated')

query = "select user_profile.user_id, webpage from user_profile left join tmp_website_description w on w.user_id = user_profile.user_id where length(webpage) > 1 and date_created > '2016-06-15' and w.user_id is null;"
users = dbGetQuery(con, query)
users$user_id = as.numeric(as.character(users$user_id))

# Fix URLs
www = which(tolower(substring(users$webpage,1,3))=="www")
users$webpage[www] = sapply(users$webpage[www], function(x) paste('http://', x, sep=''))
http = which(tolower(substring(users$webpage,1,4))!="http")
users$webpage[http] = sapply(users$webpage[http], function(x) paste('http://', x, sep=''))

# Run loop
descriptions = list()
counter = 1
step_size = 20
max_size = nrow(users)
#max_size = 400
steps = seq(1,max_size, by=step_size)
  for(i in steps){
    print(i)
    x = get_descriptions(users[i:(i+step_size-1),])
  
    # Close open connections
    all_cons <- dbListConnections(MySQL())
    for(con in all_cons) dbDisconnect(con)
    
    # Connect to db via ssh ssh stats@looker.transferwise.com -L 3307:172.18.1.43:3306
    con <- dbConnect(MySQL(),
                     user = .env$usr,
                     password = .env$pw,
                     host = '127.0.0.1',
                     port = 3307,
                     dbname='obfuscated')
    x$date_added = Sys.time()
    dbWriteTable(con, 'tmp_website_description', x, row.names=F, append=T) %>% print
    counter = counter +1
  }

x = do.call(rbind, descriptions)

# Get existing website descriptions and remove duplicates
query = "select * from tmp_website_description"
tmp_website_description = dbGetQuery(con, query)
rm = which(is.na(tmp_website_description[,2]) | nchar(tmp_website_description[,2]) < 8)
if(length(rm)>0){
  tmp_website_description = tmp_website_description[-rm,]  
}

tmp_website_description = rbind(tmp_website_description, x)
dup = which(duplicated(tmp_website_description$user_id))
if(length(dup)>0){
  tmp_website_description = tmp_website_description[-dup,]  
}
# Write to db and add index
dbWriteTable(con, 'tmp_website_description', tmp_website_description, row.names=F, append=T)
dbSendQuery(con, 'alter table tmp_website_description add index (user_id)')
