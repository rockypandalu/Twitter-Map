# Twitter-Map
An AWS web application, which enables getting data from twitter using twitter streaming API, and show the twitter geo-location on google map.
The application also support filter by keyword, as well as search for messages in a specific area. To run the program, configure elastic search on another ec2 instance and run gettweet.py.
Then connect to the instance in application.py, deploy the application in a AWS elstric benstalk. 
