using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;
using System.IO;

// what we are working with
Console.WriteLine("IMDB Import");

// lists
List<Title_Model> movies = new List<Title_Model>();
List<Genre_Model> genres = new List<Genre_Model>();
List<Name_Model> names = new List<Name_Model>();
List<NameProfession_Model> nameProfessions = new List<NameProfession_Model>();
List<NameTitle_Model> nameTitles = new List<NameTitle_Model>();
List<CrewDirector_Model> crewDirectors = new List<CrewDirector_Model>();
List<CrewWriter_Model> crewWriters = new List<CrewWriter_Model>();
int numberOfLinesToRead = 1000; // Set the number of lines to read from the file

// locate file from drive, movies and genres
void ReadTitlesAndGenres()
{
	foreach (string movie in File.ReadLines("C:/temp/title.basics.tsv").Skip(1).Take(numberOfLinesToRead))
	{ // foreach line in the file
		string[] movieParts = movie.Split('\t'); // array of parts
		string[] genreParts = new string[2]; // array set to hold two parts
		if (movieParts.Length == 9)
		{ // if the line has 9 parts, it is valid and can be added to the list
			movies.Add(new Title_Model(movieParts));
			// add a new title using the Parts array, into the movies list
			string genreString = movieParts[8]; // sets the 8th part/index from movieParts as genreString
			try
			{
				string[] genreArray = genreString.Split(','); // genreString splits into an array of genres
				foreach (string genre in genreArray)
				{ // for each string/part in the array, the parts are inserted into the genreParts array
					genreParts[0] = movieParts[0]; // TConst
					genreParts[1] = genre; // Genre
					genres.Add(new Genre_Model(genreParts)); // add a new genre to the genres list
				}
			}
			catch (Exception)
			{ // catches if the genreString could not be split
				genreParts[0] = movieParts[0]; // TConst
				genreParts[1] = movieParts[8]; // Genre (if only one genre, no comma, so it will be added as is)
				genres.Add(new Genre_Model(genreParts));
			}
		}
		else
		{
			Console.WriteLine("Invalid line: " + movie);
		}
	}
}

//locate name file and SKIP anyone with no Profession or Title
void ReadNames()
{
	foreach (string name in File.ReadLines("C:/temp/name.basics.tsv").Skip(1).Take(numberOfLinesToRead))
	{
		string[] nameParts = name.Split("\t");
		string[] professionParts = new string[2];
		string[] nameTitleParts = new string[2];
		if (nameParts.Length == 6)
		{
			names.Add(new Name_Model(nameParts));
			string professionString = nameParts[4];
			string titlesString = nameParts[5];
			try
			{
				string[] professionArr = professionString.Split(",");
				string[] titlesArr = titlesString.Split(",");
				foreach (string profession in professionArr)
				{
					professionParts[0] = nameParts[0];
					professionParts[1] = profession;
					nameProfessions.Add(new NameProfession_Model(professionParts));
				}
				foreach (string title in titlesArr)
				{
					nameTitleParts[0] = nameParts[0];
					nameTitleParts[1] = title;
					nameTitles.Add(new NameTitle_Model(nameTitleParts));
				}
			}
			catch (Exception ex)
			{
				if (nameParts[4].Equals("\\N") || nameParts[5].Equals("\\N"))
				{ // if either value is null, skip the line
					continue;
				}
				else
				{
					professionParts[0] = nameParts[0];
					professionParts[1] = nameParts[4];
					nameProfessions.Add(new NameProfession_Model(professionParts));

					nameTitleParts[0] = nameParts[0];
					nameTitleParts[1] = nameParts[5];
					nameTitles.Add(new NameTitle_Model(nameTitleParts));
				}
			}
		}
		else
		{
			Console.WriteLine("Invalid line: " + name);
		}
	}
}

// locate crew file and skip any line with no Director OR Writer
void ReadCrew()
{
	foreach (string crew in File.ReadLines("C:/temp/title.crew.tsv").Skip(1).Take(numberOfLinesToRead))
	{
		string[] parts = crew.Split("\t");
		string[] directorParts = new string[2];
		string[] writerParts = new string[2];
		if (parts.Length == 3)
		{
			string directorString = parts[1];
			string writerString = parts[2];
			try
			{
				string[] directorArr = directorString.Split(",");
				string[] writerArr = writerString.Split(",");
				foreach (string director in directorArr)
				{
					directorParts[0] = parts[0];
					directorParts[1] = director;
					crewDirectors.Add(new CrewDirector_Model(directorParts));
				}
				foreach (string writer in writerArr)
				{
					writerParts[0] = parts[0];
					writerParts[1] = writer;
					crewWriters.Add(new CrewWriter_Model(writerParts));
				}
			}
			catch (Exception ex)
			{ // if BOTH are null, skip the line
				if (!parts[1].Equals("\\N"))
				{
					directorParts[0] = parts[0];
					directorParts[1] = parts[1];
					crewDirectors.Add(new CrewDirector_Model(directorParts));
				}
				if (!parts[2].Equals("\\N"))
				{
					writerParts[0] = parts[0];
					writerParts[1] = parts[2];
					crewWriters.Add(new CrewWriter_Model(writerParts));
				}
				continue;
			}
		}
		else
		{
			Console.WriteLine("Invalid line: " + crew);
		}
	}
}

// insert actual data
//IInserter normalInserter = new NormalInserter();
//IInserter preparedInserter = new PreparedInserter();
IInserter bulkInserter = new BulkInserter();

SqlConnection sqlConn = new SqlConnection(
	"Server=localhost;Database=IMDB;Trusted_Connection=True;" +
	"Trusted_Connection=True;TrustServerCertificate=True;");

//sqlConn.Open();
//normalInserter.InsertTitles(movies, sqlConn);
//sqlConn.Close();

//sqlConn.Open();
//preparedInserter.InsertTitles(movies, sqlConn);
//preparedInserter.InsertGenres(genres, sqlConn);
//sqlConn.Close();

ReadTitlesAndGenres();
//ReadNames();
//ReadCrew();
sqlConn.Open();
bulkInserter.InsertTitles(movies, sqlConn);
//bulkInserter.InsertGenres(genres, sqlConn);
//bulkInserter.InsertNames(names, sqlConn);
//bulkInserter.InsertNameProfessions(nameProfessions, sqlConn);
//bulkInserter.InsertNameTitles(nameTitles, sqlConn);
//bulkInserter.InsertCrewDirectors(crewDirectors, sqlConn);
//bulkInserter.InsertCrewWriters(crewWriters, sqlConn);
sqlConn.Close();