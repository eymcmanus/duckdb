#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <random> 
#include <algorithm>
#include "duckdb.hpp"

using namespace duckdb; 

class Root_Pred {
	public:
	string table;
	string column; 
	char op;
	float value;
       Root_Pred(string t, string c, char o, float v)
       {
	table = t;
	column = c; 
	op = o; 
	value = v; 
       }	       
}; 


int load_partsupp(DuckDB db, Connection con) {
	string fname ("../../../data/partsupp.csv");
	con.Query("CREATE TABLE partsupp (PS_PARTKEY int, PS_SUPPKEY int, PS_AVAILQTY int, PS_SUPPLYCOST float);");
	vector<vector<string>> content; 
	vector<string> row;
	string line, word;
	
	Appender appender(con, "partsupp"); 	
	std::fstream file (fname, std::ios::in); 
	if(file.is_open())
	{
		while(getline(file, line))
		{
			row.clear(); 
			std::stringstream str(line); 
			while(getline(str, word, '|'))
				row.push_back(word);
			content.push_back(row);
		}
	}

	for(int i=0; i<content.size(); i++)
	{
	        int32_t key = std::stoi(content[i][0]); 	
	        int32_t supp_key = std::stoi(content[i][1]); 
		int32_t avail = std::stoi(content[i][2]); 
		float cost = std::stof(content[i][3]);
		string comment = content[i][4]; 	
		appender.AppendRow(key, supp_key, avail, cost); 
	}
	appender.Close(); 

	return 0; 
}

int load_test(DuckDB db, Connection con){
	con.Query("CREATE TABLE people(id INTEGER, name VARCHAR)");
	Appender appender(con, "people");
	appender.BeginRow();
	appender.Append<int32_t>(2);
	appender.Append<string>("Hannes");
	appender.EndRow();
	appender.Flush(); 
	return 0; 
}

void generate_predicates(Connection con, string table, int vol){
	vector  <string> vect;
        vector <Root_Pred> root_vect; 	
	vector <char> ops = {"=", ">"}; 
	
	unique_ptr<QueryResult> result = con.Query("SELECT * FROM partsupp");
	auto heightr = con.Query("SELECT COUNT(*) from partsupp"); 
	auto count = ((heightr->Fetch())->GetValue(0,0)).DefaultTryCastAs(LogicalType::HUGEINT);
	// put this part inside a while, until we have vol predicates
	//pick a random column
	std::mt19937 generator(std::random_device{}());
	std::uniform_int_distribution<std::size_t> distribution(0, (result->names).size() - 1);
        std::uniform_int_distribution<std::size_t> distwo(0, count-1); 	
	
	while(root_vect.size() < vol){
	std::size_t colin = distribution(generator);
	std::size_t rowin = distwo(generator);
	string col = (result->names)[colin]; 
	int flip = rand()%1; 
	char o = ops[flip]; 
	float x = ((result->Fetch())->GetValue(colin, rowin)).DefaultTryCastAs(LogicalType::HUGEINT); 
        Root_Pred new_pred = Root_Pred("partsupp", col, o, x);;
        root_vect.push_back(new_pred);
	// items in vector 
	}
	//string col = (result->names)[colin]; 
	//beginning the query result iterator via the begin on the query result
	//auto test = result->begin();
	//grab the current row from the iterator product
        //auto help = test.current_row; 	
//	std::cout>>(help.GetValue<idx_t>(1)).ToString()>>"\n";



	//unique_ptr<DataChunk> test1 = result->Fetch(); //returns pointer to datachunk type 
	//for (idx_t col = 0; col < test1-> ColumnCount(); col++){
	//	for (idx_t row = 0; row < test1->size(); row++){
	//		auto  lvalue = test1->GetValue(col, row).DefaultCastAs(LogicalType::VARCHAR);
		   //    vect.push_back(lvalue); 	
	//	}
	//}	
	//return vect; 	
}

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	//load_test(db, con); 
	load_partsupp(db, con);
	//initialize the appender

	auto result = con.Query("SELECT * FROM partsupp limit 10;");
	//result->Print();
	generate_predicates(con, "partsupp", 50);
	return 0; 
}
