
--Load the Dallas police incidents data the zip code population data
incident_file = LOAD 'project/police_incident.tsv' USING PigStorage();
       zippop = LOAD 'project/zip_totpop.csv' USING PigStorage(',') as (popzip:chararray, totpop:int);

    police_data = foreach incident_file generate (chararray) $1 as year,
                                               (chararray) $5 as watch,   
                                               (chararray) $8 as penalty_class,                                                
                                               (chararray) $16 as zipcode,
                                               (chararray) $23 as division,
					       (chararray) $27 as community,
				               (chararray) $29 as year_first_occur,
				               (chararray) $30 as month_first_occur,
				               (chararray) $31 as day_week_first_occur,
					       (chararray) $58 as com_gender,
                                               (int) $59 as com_age,
                                               (chararray) $92 as UCR_offense_name,
					       (chararray) $98 as gang_offense,
                                               (chararray) $100 as drug_offense;

b = limit police_data 5;
dump b;

--Finding frequency of crimes based on zipcode


police_dat = FILTER police_data BY zipcode is not null;
tot = group police_dat all;
total = foreach tot generate COUNT(tot.$1) as total_count;
zip = group police_dat by zipcode; 
count_zip = foreach zip generate group,COUNT(police_dat) as cnt, (double)COUNT(police_dat)/total.total_count as percentage; 

--Filtering the top zipcode with percentage of crimes more than 2.5%

top_crime_zip = FILTER count_zip by percentage > .025;
crime_zipwise = order top_crime_zip by cnt DESC;

--dump crime_zipwise;
store crime_zipwise into '/user/root/project/police_data/zipwise' using PigStorage(',');


--Exploratory Analysis of the top crime rate zipcode greater than 2.5% with year , month , day and watch 


--Analysis of the crime occuring the top notorious zipcodes distributed based on the year                  
police_year = group top_crime_zip by zipcode;
tot_year = foreach police_year generate group as location,COUNT(top_crime_zip) as total_year;

--Calculating percentage and count of crime yearwise according to the zipcode
analysis_year = foreach police_year{
year2015 = FILTER top_crime_zip BY year == '2015';
year2016 = FILTER top_crime_zip BY year == '2016';
year2017 = FILTER top_crime_zip BY year == '2017';
generate group as location_code, COUNT(year2015) as crime_2015, COUNT(year2016) as crime_2016, 
COUNT(year2017) as crime_2017;
};
analysis = join tot_year by location, analysis_year by location_code;
year_analysis = foreach analysis generate location,total_year,crime_2015,(float) crime_2015/total_year as crime_2015_percent,
crime_2016,(float) crime_2016/total_year as crime_2016_percent,
crime_2017,(float) crime_2017/total_year as crime_2017_percent;


--dump year_analysis;

store year_analysis into 'user/root/project/police_data/year_wise' using PigStorage(','); 

month_wo_null = filter top_crime_zip by month_first_occur is not null;                 
police_monthwise = group month_wo_null by zipcode;
tot_crime = foreach police_monthwise generate group as zipcode,COUNT(month_wo_null) as total_crime;

--Calculating percentage and count of month  according to the top crime occuring zipcode
analysis_month = foreach police_monthwise{
jan = FILTER month_wo_null BY month_first_occur == 'January';
feb = FILTER month_wo_null BY month_first_occur == 'February';
mar = FILTER month_wo_null BY month_first_occur == 'March';
apr = FILTER month_wo_null BY month_first_occur == 'April';
ma = FILTER month_wo_null BY month_first_occur == 'May';
jun = FILTER month_wo_null BY month_first_occur == 'June';
jul = FILTER month_wo_null BY month_first_occur == 'July';
aug = FILTER month_wo_null BY month_first_occur == 'August';
sep = FILTER month_wo_null BY month_first_occur == 'Septembe';
oct = FILTER month_wo_null BY month_first_occur == 'October';
nov = FILTER month_wo_null BY month_first_occur == 'November';
dec = FILTER month_wo_null BY month_first_occur == 'December';
generate group as location_code, COUNT(jan) as january, COUNT(feb) as february, 
COUNT(mar) as march, COUNT(apr) as april,
COUNT(ma) as may, COUNT(jun) as june,
COUNT(jul) as july, COUNT(aug) as august,
COUNT(sep) as september, COUNT(oct) as october,
COUNT(nov) as november, COUNT(dec) as december;
};
monthwise_analysis = join analysis_month by location_code,tot_crime by zipcode;
month_analyse = order monthwise_analysis by total_crime DESC;
--dump month_analyse;

store month_analyse into 'user/root/project/police_data/month_wise' using PigStorage(','); 



--Analysis of the crime occuring the top notorious zipcodes distributed based on the day

 
day_wo_null = filter top_crime_zip by day_week_first_occur is not null;                 
police_daywise = group day_wo_null by zipcode;
tot_crime = foreach police_daywise generate group as zipcode,COUNT(day_wo_null) as total_crime;

--Calculating percentage and count of day according to the zipcode
analysis_day = foreach police_daywise{
mon = FILTER day_wo_null BY day_week_first_occur == 'Mon';
tues = FILTER day_wo_null BY day_week_first_occur == 'Tue';
wed = FILTER day_wo_null BY day_week_first_occur == 'Wed';
thrus = FILTER day_wo_null BY day_week_first_occur == 'Thu';
fri = FILTER day_wo_null BY day_week_first_occur == 'Fri';
sat = FILTER day_wo_null BY day_week_first_occur == 'Sat';
sun = FILTER day_wo_null BY day_week_first_occur == 'Sun';

generate group as location_code, COUNT(mon) as monday, COUNT(tues) as tuesday, 
COUNT(wed) as wednesday, COUNT(thrus) as thrusday,
COUNT(fri) as friday, COUNT(sat) as saturday,
COUNT(sun) as sunday;
};
daywise_analysis = join analysis_day by location_code,tot_crime by zipcode;
day_analyse = order daywise_analysis by total_crime DESC;
--dump day_analyse;

store day_analyse into 'user/root/project/police_data/daywise' using PigStorage(','); 

--Analysis according to the watch and top notorious zipcodes
 
watch_wo_null = filter top_crime_zip by watch is not null;                 
police_watch = group watch_wo_null by zipcode;
tot_watch = foreach police_watch generate group as location,COUNT(watch_wo_null) as total_watch;

--Calculating percentage and count of watch according to the zipcode

analysis_watch = foreach police_watch{
late = FILTER watch_wo_null BY watch == '1';
day = FILTER watch_wo_null BY watch == '2';
even = FILTER watch_wo_null BY watch == '3';
unknow = FILTER watch_wo_null BY watch == 'U';
generate group as location_code, COUNT(late) as late_night, COUNT(day) as daylight, 
COUNT(even) as evening, COUNT(unknow) as Unknown;
};
analysis = join tot_watch by location, analysis_watch by location_code;
watch_analysis = foreach analysis generate location,total_watch,late_night,(float) late_night/total_watch as late_night_percent,
daylight,(float) daylight/total_watch as daylight_percent,
evening,(float) evening/total_watch as evening_percent,
Unknown,(float) Unknown/total_watch as Unknown_percent;
watch_analysis = order watch_analysis by total_watch DESC;

--dump watch_analysis;

store watch_analysis into 'user/root/project/police_data/watch_wise' using PigStorage(','); 
