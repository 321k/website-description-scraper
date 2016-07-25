# Load required packages
library(RMySQL)
library(dplyr)
library(rvest)
library(RCurl)


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

# Select new businesses with a website
query = "select user_profile.user_id, webpage from user_profile left join tmp_website_description w on w.user_id = user_profile.user_id where length(webpage) > 1 and date_created > '2016-06-01' and w.user_id is null;"
users = dbGetQuery(con, query)

users$user_id = as.numeric(as.character(users$user_id))

# Fix URLs
www = which(tolower(substring(users$webpage,1,3))=="www")
users$webpage[www] = sapply(users$webpage[www], function(x) paste('http://', x, sep=''))
http = which(tolower(substring(users$webpage,1,4))!="http")
users$webpage[http] = sapply(users$webpage[http], function(x) paste('http://', x, sep=''))

# Create list to store descriptions if it doesn't exist
description = list()

# Fetch descriptions from companies' websites
for(i in (length(description)+1):nrow(users)){
  print(paste(i, i/nrow(users)))
  
  result = tryCatch({
     read_html(users$webpage[i])
  }, warning = function(w) {
    read_html(users$webpage[i])
  }, error = function(e) {
    "error"
  }, finally = {
  })
  
  if(result[1]=='error') next()
  description[[i]] = result %>% html_nodes('meta[name=description]') %>% as.character
  
  
  if(length(description[[i]])==0){
    description[[i]] = result %>% html_nodes('p[class="hero textalign-c"]') %>% as.character
  }
  
  if(length(description[[i]])==0){
    description[[i]] = result %>% html_nodes('title') %>% as.character
  }
  print(description[[i]])
}

# Create a backup just in case
description_save = description
#description = description_save

# If there are multiple metatags named desription, select the last one
for(i in 1:length(description)){
  k = length(description[[i]])
  if(k == 0){
    description[[i]] = NA
  } else {
  description[[i]] = description[[i]][k]
  }
}

# Convert list to vector and clean out HTML

content_start = sapply(description, function(x) gregexpr('content=', x, ignore.case = TRUE))
content_start = sapply(content_start, `[[`, 1)
names(content_start) = 1:length(content_start)
content_node = grep('content', description, ignore.case=T)
description[content_node] = substring(description[content_node], content_start[content_node]+7, nchar(description[content_node]))

description = unlist(description)
names(description) = 1:length(description)

description = sapply(description, URLdecode)
description = gsub("<.*?>", '', description, ignore.case=T)
description = gsub("[^0-9A-Za-z.,///' ]", "", description)
description = gsub("/", "", description)
description = gsub("[ ]{2,}", "", description)

# Add user id
x = data.frame(user_id = users$user_id[1:length(description)], website_description = description)

# Close open connections and open a new one
all_cons <- dbListConnections(MySQL())
for(con in all_cons) dbDisconnect(con)
con <- dbConnect(MySQL(),
                 user = .env$usr,
                 password = .env$pw,
                 host = '127.0.0.1',
                 port = 3307,
                 dbname='obfuscated')


# Write to db and add index
dbWriteTable(con, 'tmp_website_description', x, row.names=F, append=T)
dbSendQuery(con, 'alter table tmp_website_description add index (user_id)')
