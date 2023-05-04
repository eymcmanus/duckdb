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
	Value value;
       Root_Pred(string t, string c, char o, float v)
       {
	table = t;
	column = c; 
	op = o; 
	value = v; 
       }
	string ToString() const;        
}; 

string Root_Pred::ToString() const{
	std::stringstream ss; 
	ss<<table<<"."<<column<<" "<<op<<" "<<value; 
	return ss.str(); 

}
 
class And_Root_Pred {
    public:
        Root_Pred left;
        Root_Pred right;
        And_Root_Pred(Root_Pred l, Root_Pred r) : left(l), right(r) {}

	std::string ToString() const{
		std::stringstream ss; 
		ss << "(" << left.ToString() << " AND " << right.ToString() << ")"; 
		return ss.str(); 
	}
};

void print_vector(const duckdb::vector<bool>& vec){
	for (auto val: vec){
		std::cout<< val << " "; 
	}
	

	}



bool evaluate_predicate(const Root_Pred& pred,  const duckdb::Value& val) {
    switch (pred.op) {
        case '=':
            return val.DefaultCastAs(pred.value.type()) == pred.value;
        case '>':
            return val.DefaultCastAs(pred.value.type()) >= pred.value;
        default:
            throw std::runtime_error("Unsupported predicate operation");
	}

}
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

vector <And_Root_Pred>  generate_predicates(Connection con, string table, int vol){
	vector  <string> vect;
        vector <Root_Pred> root_vect; 	
	vector <char> ops = {'=', '>'}; 
	
	vector <And_Root_Pred> and_preds; 
		
	unique_ptr<QueryResult> result = con.Query("SELECT * FROM partsupp");
	auto heightr = con.Query("SELECT COUNT(*) from partsupp"); 
	auto count = ((heightr->Fetch())->GetValue(0,0)).DefaultTryCastAs(duckdb::LogicalType::HUGEINT); 
	std::size_t count_size_t = static_cast<std::size_t>(count); 
	// put this part inside a while, until we have vol predicates
	//pick a random column
	std::mt19937 generator(std::random_device{}());
	std::uniform_int_distribution<std::size_t> distribution(0, (result->names).size() - 1);
        std::uniform_int_distribution<std::size_t> distwo(0, count_size_t-1); 	
	while(root_vect.size() < (vol*2)){
		std::size_t colin = distribution(generator);
		std::size_t rowin = distwo(generator);
		string col = (result->names)[colin]; 
		int flip = rand()%2; 
		char o = ops[flip];
		float x = static_cast<float>(std::stod(result->Fetch()->GetValue(colin, rowin).ToString()));
		Root_Pred new_pred = Root_Pred("partsupp", col, o, x);;
        	root_vect.push_back(new_pred);
	}
	// now to generate the and predicates 
	std::random_device rd; 
	std::mt19937 g(rd()); 
	std::shuffle(root_vect.begin(), root_vect.end(), g); 

	std::uniform_int_distribution<int> dist(0, root_vect.size()-1); 
	for (int i = 0; i < vol ; i ++){
		int idx1 = dist(g); 
		int idx2 = dist(g); 
		and_preds.emplace_back(root_vect[idx1], root_vect[idx2]); 
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
	return and_preds; 	
}


vector<vector<bool>> generate_one_hot_vectors(Connection con,vector<And_Root_Pred> preds){
	
	auto result = con.Query("SELECT * from partsupp");  
	vector<vector<bool>> one_hot_vectors(result->Fetch()->ColumnCount(), vector<bool>(result->Fetch()->size())); 

	vector<string> col_names = result->names;
	vector<LogicalType> col_types = result->types; 

	while(auto chunk =  result->Fetch()){
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++){
			auto &col = chunk->data[row_idx]; 
			for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++){
				bool match = true;
				auto& col_name = col_names[col_idx]; 
				const auto& val = chunk->GetValue(col_idx, row_idx);
				for (const auto& pred : preds){
					if (col_name != pred.left.column){
						continue;
					}
					if (!evaluate_predicate(pred.left, val) || !evaluate_predicate(pred.right, val)){
						match = false; 
						break;
					}}
				if (match){
					one_hot_vectors[col_idx][row_idx] = true;
				}

			}
		}
	}
	return one_hot_vectors; 
}
int main() {
	DuckDB db(nullptr);
	Connection con(db);
	//load_test(db, con); 
	load_partsupp(db, con);
	//initialize the appender

	auto result = con.Query("SELECT * FROM partsupp limit 10;");
	//result->Print();
	auto pred = generate_predicates(con, "partsupp", 100);
	auto interventions = generate_one_hot_vectors(con, pred);
	print_vector(interventions[2]);
	return 0; 
}
