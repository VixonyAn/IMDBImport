using IMDBImport;
using Microsoft.Data.SqlClient;

// what we are working with
Console.WriteLine("IMDB Import");

// list
List<Title_Model> movies = new List<Title_Model>();
List<Genre_Model> genres = new List<Genre_Model>();

// locate file from drive
foreach (string movie in File.ReadLines("C:/temp/title.basics.tsv").Skip(1).Take(10))
{
	string[] parts = movie.Split('\t');
	if (parts.Length == 9)
	{
		Title_Model title = new Title_Model(parts);
		movies.Add(title);
	}
	else
	{
		Console.WriteLine("Invalid line: " + movie);
	}
}

// print movies
foreach (var movie in movies)
{
	Console.WriteLine(movie.ToString());
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
//sqlConn.Close();

sqlConn.Open();
bulkInserter.InsertTitles(movies, sqlConn);
sqlConn.Close();