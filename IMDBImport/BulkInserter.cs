using IMDBImport.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport
{
	public class BulkInserter : IInserter
	{
		public void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn)
		{
			DataTable titleTable = new DataTable();
			titleTable.Columns.Add("TConst", typeof(string));
			titleTable.Columns.Add("TitleType", typeof(string));
			titleTable.Columns.Add("PrimaryTitle", typeof(string));
			titleTable.Columns.Add("OriginalTitle", typeof(string));
			titleTable.Columns.Add("IsAdult", typeof(bool));
			titleTable.Columns.Add("StartYear", typeof(int));
			titleTable.Columns.Add("EndYear", typeof(int));
			titleTable.Columns.Add("Runtime", typeof(int));

			foreach (Title_Model movie in titles)
			{
				titleTable.Rows.Add(movie.TConst,
					movie.TitleType,
					movie.PrimaryTitle,
					movie.OriginalTitle,
					movie.IsAdult,
					movie.StartYear ?? (object)DBNull.Value,
					movie.EndYear ?? (object)DBNull.Value,
					movie.Runtime ?? (object)DBNull.Value
				);
			}

			SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn)
			{
				DestinationTableName = "Titles"
			};
		}
		public void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn)
		{
			DataTable genreTable = new DataTable();
			genreTable.Columns.Add("TConst", typeof(string));
			genreTable.Columns.Add("Genre", typeof(string));

			foreach (Genre_Model genre in genres)
			{
				genreTable.Rows.Add(genre.TConst,
					genre.Genre
				);
			}

			SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn)
			{
				DestinationTableName = "Title_Genres"
			};
		}
		public void InsertNames(List<Name_Model> names, SqlConnection sqlConn)
		{
			DataTable nameTable = new DataTable();
			nameTable.Columns.Add("NConst", typeof(string));
			nameTable.Columns.Add("PrimaryName", typeof(string));
			nameTable.Columns.Add("BirthYear", typeof(int));
			nameTable.Columns.Add("DeathYear", typeof(int));

			foreach (Name_Model name in names)
			{
				nameTable.Rows.Add(name.NConst,
					name.PrimaryName,
					name.BirthYear ?? (object)DBNull.Value,
					name.DeathYear ?? (object)DBNull.Value
				);
			}

			SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn)
			{
				DestinationTableName = "Names"
			};
		}
		public void InsertNameTitles(List<NameTitle_Model> nameTitles, SqlConnection sqlConn)
		{
			DataTable nameTitleTable = new DataTable();
			nameTitleTable.Columns.Add("TConst", typeof(string));
			nameTitleTable.Columns.Add("NConst", typeof(string));

			foreach (NameTitle_Model nameTitle in nameTitles)
			{
				nameTitleTable.Rows.Add(nameTitle.TConst,
					nameTitle.NConst
				);
			}

			SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn)
			{
				DestinationTableName = "Name_Titles"
			};
		}
		public void InsertNameProfessions(List<NameProfession_Model> nameProfessions, SqlConnection sqlConn)
		{
			DataTable nameProfessionTable = new DataTable();
			nameProfessionTable.Columns.Add("NConst", typeof(string));
			nameProfessionTable.Columns.Add("PrimaryProfession", typeof(string));

			foreach (NameProfession_Model nameProfession in nameProfessions)
			{
				nameProfessionTable.Rows.Add(nameProfession.NConst,
					nameProfession.PrimaryProfession
				);
			}

			SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn)
			{
				DestinationTableName = "Name_Professions"
			};
		}
		public void InsertCrewDirectors(List<CrewDirector_Model> crewDirectors, SqlConnection sqlConn)
		{
			DataTable crewDirectorTable = new DataTable();
			crewDirectorTable.Columns.Add("TConst", typeof(int));
			crewDirectorTable.Columns.Add("NConst", typeof(int));

			foreach (CrewDirector_Model director in crewDirectors)
			{
				crewDirectorTable.Rows.Add(director.TConst, director.NConst);
			}
			SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn)
			{
				DestinationTableName = "Crew_Directors"
			};
			bulkCopy.WriteToServer(crewDirectorTable);
		}
		public void InsertCrewWriters(List<CrewWriter_Model> crewWriters, SqlConnection sqlConn)
		{
			DataTable crewWriterTable = new DataTable();
			crewWriterTable.Columns.Add("TConst", typeof(int));
			crewWriterTable.Columns.Add("NConst", typeof(int));

			foreach (CrewWriter_Model writer in crewWriters)
			{
				crewWriterTable.Rows.Add(writer.TConst, writer.NConst);
			}
			SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn)
			{
				DestinationTableName = "Crew_Writers"
			};
			bulkCopy.WriteToServer(crewWriterTable);
		}
	}
}
