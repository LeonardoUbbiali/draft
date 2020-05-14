#set the parameters
tableBQid<-#yourBQtableID
startDate<- #startDate# (Format:AAAAMMDD)
endDate<- #endDate# (Format:AAAAMMDD)
checkoutStep <- #n (checkout step 1/2/3/.../n)

#DATA RETRIEVAL-----
#connect with BigQuery
connessione<- dbConnect(bigquery(),project='#yourBQproject')

#Query for data retrieval
query <- paste0('WITH Drops as (
                SELECT
                date,
                country,
                device,
                COUNT(*) as checkmailDropoffs
                FROM(
                SELECT
                date,
                geoNetwork.country as country,
                device.deviceCategory as device,
                CONCAT(fullVisitorId, CAST(visitId as STRING)) as sessionId ,
                (MAX(ecommerceaction.step)) AS maxStep
                FROM `yoox-bq-export.',storeBQid,'.ga_sessions_*`, UNNEST(hits) as hit
                WHERE _TABLE_SUFFIX BETWEEN "',startDate,'" AND "',endDate,'"
                GROUP BY 1,2,3,4)
                WHERE maxStep =',checkoutStep,' 
                GROUP BY 1,2,3),
                Sessions as (
                SELECT
                date,
                country,
                device,
                count(*) as checkmailSessions
                FROM(
                SELECT
                date,
                geoNetwork.country as country,
                device.deviceCategory as device,
                CONCAT(fullVisitorId, CAST(visitId as STRING)) as sessionId ,
                ecommerceaction.step AS Step
                FROM `yoox-bq-export.',storeBQid,'.ga_sessions_*`, UNNEST(hits) as hit
                WHERE _TABLE_SUFFIX BETWEEN "',startDate,'" AND "',endDate,'"
                AND ecommerceaction.step = ',checkoutStep,' 
                GROUP BY 1,2,3,4,5)
                GROUP BY 1,2,3)
                SELECT
                Sessions.date as Date,
                Sessions.country as Country,
                Sessions.device as Device,
                Sessions.checkmailSessions  AS stepSessions,
                checkmailDropoffS AS stepDrops
                FROM
                Sessions LEFT JOIN  Drops ON Drops.date = Sessions.date  AND Drops.country = Sessions.country  AND Drops.device = Sessions.device 
                ')

#executing the query on BigQuery
dataset<-DBI::dbGetQuery(connessione,query)

#DATA WRANGLING----- 

#converting "Date" in date type
dataset$Date<-as.Date(dataset$Date, "%Y%m%d")

#rimozione NA
dataset[is.na(dataset)] <-0

#raggruppo per le dimensioni di interesse e calculo DropOff rate
ds<-dataset%>%
  group_by(Date)%>%
  summarise(stepSessions = sum(stepSessions),
            stepDrops = sum (stepDrops),
            dropRate = round(stepDrops/stepSessions,2))

#DATAVIZ-----
#static line plot with ggplot base giorno
staticPlot<-ds%>%
  ggplot(aes(Date, dropRate))+
  geom_line()+
  geom_smooth()+
  labs(title = "step dropoff rate time series")+
  theme_light()+scale_y_continuous(breaks = seq(0, 0.8, by = 0.1))

staticPlot
#dinamic line plot with plotly
dynamicPlot<-ggplotly(staticPlot)
dynamicPlot
