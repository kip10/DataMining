using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;

namespace makeDatabase
{
    class Program
    {
        static MySqlConnection conn = new MySqlConnection("Server=54.187.49.196;Database=climate;Uid=root;Pwd=435project");

        static void Main(string[] args)
        {
            insertWeather();
            Console.ReadKey();
        }

        static void insertCountries()
        {
            MySqlCommand cmd = new MySqlCommand("INSERT INTO `climate`.`countries` (`abbreviation`,`name`) VALUES (@abbrev,@name);", conn);

            conn.Open();
            System.IO.StreamReader countries = new System.IO.StreamReader("Y:\\country-list.txt");
            System.IO.StreamWriter csvCountries = new System.IO.StreamWriter("Y:\\country-list.csv");

            Console.WriteLine(countries.ReadLine());
            Console.WriteLine(countries.ReadLine());

            while (countries.Peek() > 0)
            {
                string line = countries.ReadLine();
                string[] strings = line.Split(new string[] { "          " }, StringSplitOptions.RemoveEmptyEntries);
                //string[] strings = line.Split(new char[] { ' ' }, 12);
                Console.WriteLine(strings[0] + "," + strings[1]);
                csvCountries.WriteLine(strings[0] + "," + strings[1]);
                cmd.Parameters.AddWithValue("abbrev", strings[0]);
                cmd.Parameters.AddWithValue("name", strings[1]);

                cmd.ExecuteScalar();
                cmd.Parameters.Clear();
            }

            conn.Close();
            csvCountries.Close();
            countries.Close();
        }

        static void insertStations()
        {
            MySqlCommand cmd = new MySqlCommand("INSERT INTO `climate`.`stations` (`USAF`,`WBAN`,`station`,`country`,`FIPS`,`state`,`call`,`latitude`,`longitude`,`elevation`) "
                + "VALUES (@usaf,@wban,@station,@country,@fips,@state,@call,@lat,@lon,@elev);", conn);

            conn.Open();
            System.IO.StreamReader stations = new System.IO.StreamReader("Y:\\ish-history.csv");

            Console.WriteLine(stations.ReadLine());

            while (stations.Peek() > 0)
            {
                string line = stations.ReadLine();
                string[] strings = line.Split(new char[] { ',' }, 12);
                for(int i = 0; i<10; i++)
                {
                    strings[i] = strings[i].Replace("\"", string.Empty);
                }

                int c = Convert.ToInt32(strings[0]);
                if (c > 61240)
                {
                    //Console.WriteLine(strings[0] + "," + strings[1]+","+strings[2]+","+strings[3]+","+strings[4]+","+strings[5]+","+strings[6]+","+strings[7]+","+strings[8]+","+strings[9]);

                    cmd.Parameters.AddWithValue("usaf", strings[0]);
                    cmd.Parameters.AddWithValue("wban", strings[1]);
                    cmd.Parameters.AddWithValue("station", strings[2]);
                    cmd.Parameters.AddWithValue("country", strings[3]);
                    cmd.Parameters.AddWithValue("fips", strings[4]);
                    cmd.Parameters.AddWithValue("state", strings[5]);
                    cmd.Parameters.AddWithValue("call", strings[6]);
                    cmd.Parameters.AddWithValue("lat", strings[7]);
                    cmd.Parameters.AddWithValue("lon", strings[8]);
                    cmd.Parameters.AddWithValue("elev", strings[9]);

                    try
                    {
                        cmd.ExecuteScalar();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    cmd.Parameters.Clear();
                }
                Console.WriteLine(strings[0]);
            }

            conn.Close();
            stations.Close();
        }

        static void insertWeather()
        {
            List<string> stations = new List<string>();
            MySqlCommand cmd = new MySqlCommand("SELECT USAF FROM climate.stations WHERE state = 'OH'", conn);
            conn.Open();
            MySqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                stations.Add(reader.GetString(0));
            }
            conn.Close();

            List<string> stringArrs = new List<string>();
            for (int i = 1929; i < 1980; i++)
            {
                Console.WriteLine("Now on year: " + i.ToString());
                string[] files = System.IO.Directory.GetFiles("Y:\\gsod\\" + i);
                foreach (string file in files)
                {
                    string stationID = file.Substring(13,6);
                   // int year = Convert.ToInt32(file.Substring(26, 4));
                    if(stations.Contains(stationID))
                        stringArrs.Add(file);
                }
            }

            Console.WriteLine("Total of " + stringArrs.Count + " files for Ohio found.");

            int c = -1;
            int j = 0;
            foreach (string files in stringArrs)
            {
                Console.Clear();
                c++;
                if (c < 65)
                {
                    c++;
                }
                else
                {
                    Console.WriteLine(files + " :: " + c.ToString() + " out of 1032");
                    writeLine(files);
                }
                
            }
        }

        static void writeLine(string files)
        {
            //Console.WriteLine("File " + Array.IndexOf(files, file).ToString() + " out of " + files.Length);
            //Console.WriteLine(file);
            //Console.WriteLine(files.)
            System.IO.StreamReader reader = new System.IO.StreamReader(files);
            reader.ReadLine();

            MySqlCommand cmd = new MySqlCommand("INSERT INTO `climate`.`ohweather` (`station`,`wban`,`year`,`month`,`day`,`temperature`,`tempCount`,`dewPoint`,`dewPointCount`,`SLP`,`SLPCount`,"
                + "`STP`,`STPCount`,`VISIB`,`VISIBCount`,`WDSP`,`WDSPCount`,`MXSPD`,`GUST`,`MAX`,`MIN`,`PRCP`,`SNDP`,`fog`,`rain`,`snow`,`hail`,`thunder`,`tornado`) VALUES "
                + "(@station,@wban,@year,@month,@day,@temp,@tempCount,@dp,@dpc,@slp,@slpc,@stp,@stpc,@vis,@visc,@wdsp,@wdspc,@mxspd,@gust,@max,@min,@prcp,@sndp,@fog,@rain,@snow,@hail,@thunder,@tornado);", conn);

            conn.Open();

            while (reader.Peek() > 0)
            {
                string line = reader.ReadLine();//.Split(new char[]{' '});
                cmd.Parameters.AddWithValue("station", getSubstring(line, 1, 6));
                cmd.Parameters.AddWithValue("wban", getSubstring(line, 8, 12));
                cmd.Parameters.AddWithValue("year", getSubstring(line, 15, 18));
                cmd.Parameters.AddWithValue("month", getSubstring(line, 19, 20));
                cmd.Parameters.AddWithValue("day", getSubstring(line, 21, 22));
                cmd.Parameters.AddWithValue("temp", getSubstring(line, 25, 30));
                cmd.Parameters.AddWithValue("tempCount", getSubstring(line, 32, 33));
                cmd.Parameters.AddWithValue("dp", getSubstring(line, 36, 41));
                cmd.Parameters.AddWithValue("dpc", getSubstring(line, 43, 44));
                cmd.Parameters.AddWithValue("slp", getSubstring(line, 47, 52));
                cmd.Parameters.AddWithValue("slpc", getSubstring(line, 54, 55));
                cmd.Parameters.AddWithValue("stp", getSubstring(line, 58, 63));
                cmd.Parameters.AddWithValue("stpc", getSubstring(line, 65, 66));
                cmd.Parameters.AddWithValue("vis", getSubstring(line, 69, 73));
                cmd.Parameters.AddWithValue("visc", getSubstring(line, 75, 76));
                cmd.Parameters.AddWithValue("wdsp", getSubstring(line, 79, 83));
                cmd.Parameters.AddWithValue("wdspc", getSubstring(line, 85, 86));
                cmd.Parameters.AddWithValue("mxspd", getSubstring(line, 89, 93));
                cmd.Parameters.AddWithValue("gust", getSubstring(line, 96, 100));
                cmd.Parameters.AddWithValue("max", getSubstring(line, 103, 108));
                cmd.Parameters.AddWithValue("min", getSubstring(line, 111, 116));
                cmd.Parameters.AddWithValue("prcp", getSubstring(line, 119, 123));
                cmd.Parameters.AddWithValue("sndp", getSubstring(line, 126, 130));
                cmd.Parameters.AddWithValue("fog", getSubstring(line, 133, 133));
                cmd.Parameters.AddWithValue("rain", getSubstring(line, 134, 134));
                cmd.Parameters.AddWithValue("snow", getSubstring(line, 135, 135));
                cmd.Parameters.AddWithValue("hail", getSubstring(line, 136, 136));
                cmd.Parameters.AddWithValue("thunder", getSubstring(line, 137, 137));
                cmd.Parameters.AddWithValue("tornado", getSubstring(line, 138, 138));
                try
                {
                    cmd.ExecuteScalar();
                    //Console.WriteLine(getSubstring(line, 15, 22));
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
                finally
                {
                    cmd.Parameters.Clear();
                }
            }
            Console.WriteLine("Finished file");
            conn.Close();
        }

        static string getSubstring(string s, int start, int end)
        {
            int diff = end - start + 1;
            return s.Substring(start - 1, diff);
        }
    }
}
