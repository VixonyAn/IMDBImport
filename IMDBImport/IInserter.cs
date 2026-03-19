using Microsoft.Data.SqlClient;

namespace IMDBImport
{
	public interface IInserter
	{
		void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn);
		void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn);
	}
}