using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport
{
	public class PreparedInserter : IInserter
	{
		public void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn)
		{
			string query = "INSERT INTO Genres (" +
						   "TConst, " +
						   "Genre)" +
						   "VALUES (@TConst, @Genre)";
			SqlCommand sqlComm = new SqlCommand(query, sqlConn);
			sqlComm.Prepare();
			foreach (Genre_Model genre in genres)
			{
				sqlComm.Parameters.AddWithValue("@TConst", genre.TConst);
				sqlComm.Parameters.AddWithValue("@Genre", genre.Genre);
			}
			try
			{
				sqlComm.ExecuteNonQuery();
			}
			catch (SqlException ex)
			{
				Console.WriteLine("Error inserting query:\r\n" + query);
			}
		}

		public void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn)
		{
			foreach (Title_Model movie in titles)
			{
				string query = $"INSERT INTO Titles (" +
								"TConst, " +
								"TitleType, " +
								"PrimaryTitle, " +
								"OriginalTitle, " +
								"IsAdult, " +
								"StartYear, " +
								"EndYear, " +
								"Runtime) " +
									"VALUES (@TConst, @TitleType, @PrimaryTitle, " +
									"@OriginalTitle, @IsAdult, @StartYear, @EndYear, @Runtime)";
				SqlCommand sqlComm = new SqlCommand(query, sqlConn);
				sqlComm.Prepare();
				{
					sqlComm.Parameters.AddWithValue("@TConst", movie.TConst);
					sqlComm.Parameters.AddWithValue("@TitleType", movie.TitleType);
					sqlComm.Parameters.AddWithValue("@PrimaryTitle", movie.PrimaryTitle);
					sqlComm.Parameters.AddWithValue("@OriginalTitle", movie.OriginalTitle);
					sqlComm.Parameters.AddWithValue("@IsAdult", movie.IsAdult);
					sqlComm.Parameters.AddWithValue("@StartYear", (object)movie.StartYear ?? DBNull.Value);
					sqlComm.Parameters.AddWithValue("@EndYear", (object)movie.EndYear ?? DBNull.Value);
					sqlComm.Parameters.AddWithValue("@Runtime", (object)movie.Runtime ?? DBNull.Value);
					try
					{
						sqlComm.ExecuteNonQuery();
					}
					catch (SqlException sqlex)
					{
						Console.WriteLine("Error inserting new query: \r\n" + query);
						Console.WriteLine(sqlex.Message);
						Console.WriteLine("Parameters:");
						foreach (SqlParameter param in sqlComm.Parameters)
						{
							Console.WriteLine($"{param.ParameterName}: {param.Value}");
						}
					}
				}
			}
		}
	}
}
