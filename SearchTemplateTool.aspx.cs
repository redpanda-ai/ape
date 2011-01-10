using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

public partial class SamplePage : System.Web.UI.Page {
	protected void SearchButton_Click(object sender, EventArgs e) {


//This is the main template, all queries are ultimately derived from this 
		string jsonQuery = @"
{
	""from"" : _from,
	""size"" : _size,
	""fields"" : [""_return_fields""],
	""sort"" : {
		""_sort_field"" _sort_order
	},
	""query"" : {
		""filtered"" : {
			""query"" : {
				_main_search
			},
			""filter"" : {
				""and"" : [
						{
							""query"" : {
								""query_string"" : {
									""fields"" : [""targeted_countries""],
									""query"" : ""All _country_code""
								}
							}
						} _featured_items _filters
				]
			}
		}
	},
	""facets"" : {
		""vendor"" : {
			""terms"" : {
				""field"" : ""vendor.id_plus_name"",
				""size"" : 1000,
				""analyzer"" : ""none""
			},
		""global"" : false
		}
	}
}";
//				""script"" : ""term + '#' + doc['vendor.name.stored'].value""

		BuildSearchOptions(ref jsonQuery);
		BuildQueryStandardOptions(ref jsonQuery);
		BuildQueryFilterOptions(ref jsonQuery);
		BuildQueryFeaturedItems(ref jsonQuery);
		string url = GetURL();
		Results.Text = ExecuteJSON(jsonQuery, url);
		//Results.Text = "foo";
		Query.Text = "curl " + url + " -d '" + jsonQuery + "'";
	}
	protected string ExecuteJSON(string jsonQuery, string url) {
		WebRequest request = WebRequest.Create (url);
		request.Method = "POST";
		string postData = jsonQuery;
		byte[] byteArray = Encoding.UTF8.GetBytes (postData);
		request.ContentType = "application/x-www-form-urlencoded";
		request.ContentLength = byteArray.Length;
		Stream dataStream = request.GetRequestStream ();
		dataStream.Write (byteArray, 0, byteArray.Length);
		dataStream.Close ();
		WebResponse response = request.GetResponse ();
		Console.WriteLine (((HttpWebResponse)response).StatusDescription);
		dataStream = response.GetResponseStream ();
		StreamReader reader = new StreamReader (dataStream);
		string responseFromServer = reader.ReadToEnd ();
		Console.WriteLine (responseFromServer);
		reader.Close ();
		dataStream.Close ();
		response.Close ();
		return responseFromServer.ToString();
	}
	protected string GetURL() {
		string url = "http://";
		if (NodeName.Text != "") { url += NodeName.Text; }
			else { url += "cloud1"; }
		url += ":9200/";
		if (IndexName.Text != "") { url += IndexName.Text; }
			else { url += "bcproducts"; }
		url += "/";
		if (TypeName.Text != "") { url += TypeName.Text; }
			else { url += "3196"; }
		url += "/_search";
		return url;
	}
	protected void ParseVendorFilters (
		ref Dictionary<string, string> vendorFilters) {
		if (Vendor1.Text != "") { vendorFilters.Add("Vendor1",Vendor1.Text); }
		if (Vendor2.Text != "") { vendorFilters.Add("Vendor2",Vendor2.Text); }
		if (Vendor3.Text != "") { vendorFilters.Add("Vendor3",Vendor3.Text); }
	}
	protected void ParseCategoryGroupOptions (
		ref Dictionary<string,string> categoryGroupOptions ) {
		if (CGO_HostSpecies.Text != "") {
			categoryGroupOptions.Add("Host Species",CGO_HostSpecies.Text);
		} if (CGO_Application.Text != "") {
			categoryGroupOptions.Add("Applications",CGO_Application.Text);
		} if (CGO_Conjugate.Text != "") {
			categoryGroupOptions.Add("Conjugate",CGO_Conjugate.Text);
		} if (CGO_Reactivity.Text != "") {
			categoryGroupOptions.Add("Reactivity",CGO_Reactivity.Text);
		} if (CGO_AntibodyProducts.Text != "") {
			categoryGroupOptions.Add("Antibody Products"
				,CGO_AntibodyProducts.Text);
		}
	}
	protected void BuildQuerySearchFields ( ref string jsonQuery) {
		Dictionary<string, string> searchFields 
			= new Dictionary<string, string>();

		if (SF1.Text == "_id") { searchFields.Add("SF1",SF1.Text); } 
		else if (SF1.Text != "") { searchFields.Add("SF1",SF1.Text); }
		if (SF2.Text != "") { searchFields.Add("SF2",SF2.Text); }
		if (SF3.Text != "") { searchFields.Add("SF3",SF3.Text); }

		Regex re_search_fields = new Regex("_fieldstosearch_",
			RegexOptions.Compiled);

		jsonQuery = 
				re_search_fields.Replace(
				jsonQuery,ComposeSearchFields(searchFields));
	}
	protected void BuildSearchOptions ( ref string jsonQuery) {
		//Just return match_all if the search field is blank :)
		string match_all = @" ""match_all"" : { } ";
		Regex re_main_search = new Regex("_main_search",RegexOptions.Compiled);
		if (SearchBox.Text == "") {
			jsonQuery = re_main_search.Replace(jsonQuery, match_all);
			return;
		} 

		//Add a query_string and substitute _search_field with the provided term 
		string search_template =@"
""query_string"" : {
	""fields"" : [_fieldstosearch_],
	""query"" : ""_search_field""
}";
		Regex re_search = new Regex("_search_field",RegexOptions.Compiled);
		search_template = re_search.Replace(search_template, SearchBox.Text);
		BuildQuerySearchFields(ref search_template);
		
		//substitute back into the main jsonQuery template
		jsonQuery = re_main_search.Replace(jsonQuery, search_template);
	}
	protected void BuildQueryStandardOptions ( ref string jsonQuery ) {
		bool res = false;
		int pageNumber = 1;
		int pageSize = 60;
		string sortField = "_score";
		string sortOrder = ": { }";
		string rf = "_source";

		res = int.TryParse(PageNumber.Text, out pageNumber);
		if (res == false) { pageNumber = 1; }
		res = int.TryParse(PageSize.Text, out pageSize);
		if (res == false) { pageSize = 60; }
		if (SortField.Text != "") { sortField = SortField.Text; }
		if (SortOrder.Text != "") { 
			sortOrder = @": """ + SortOrder.Text + @""""; 
		}
		if (ReturnFields.Text != "") { rf = ReturnFields.Text; }

		Regex re_from = new Regex("_from",RegexOptions.Compiled);
		Regex re_size = new Regex("_size",RegexOptions.Compiled);
		Regex re_sort = new Regex("_sort_field",RegexOptions.Compiled);
		Regex re_order = new Regex("_sort_order",RegexOptions.Compiled);
		Regex re_country_code = new Regex("_country_code",RegexOptions.Compiled);
		Regex re_return_field = new Regex("_return_fields",RegexOptions.Compiled);
		
		int startIndex = pageSize * (pageNumber - 1);
		jsonQuery = re_from.Replace(jsonQuery,startIndex.ToString());
		jsonQuery = re_size.Replace(jsonQuery,pageSize.ToString()); 
		jsonQuery = re_sort.Replace(jsonQuery, sortField);
		jsonQuery = re_order.Replace(jsonQuery, sortOrder);
		jsonQuery = re_country_code.Replace(jsonQuery, CountryCode.Text);
		jsonQuery = re_return_field.Replace(jsonQuery, rf);
	}
	protected void BuildQueryFeaturedItems ( ref string jsonQuery ) {
		string result = @"
,
{
	""query"" : {
		""range"" : {
				""featured_item_start_date"" : {
				""lte"" : ""_featured_item_date""
				}
			}
	}
},
{
	""query"" : {
		""range"" : {
				""featured_item_end_date"" : {
				""gte"" : ""_featured_item_date""
				}
			}
	}
} 
"; 
		Regex re_featured_item = new Regex("_featured_items");
		Regex re_featured_item_date = new Regex("_featured_item_date");
		if (FeaturedItemDate.Text == "") {
			jsonQuery = re_featured_item.Replace(jsonQuery,"");
		} else {
			result = re_featured_item_date.Replace(result,FeaturedItemDate.Text);
			jsonQuery = re_featured_item.Replace(jsonQuery,result);
		}
	}
	protected void BuildQueryFilterOptions ( ref string jsonQuery) {

		//Create a dictionary of options
		Dictionary<string, string> categoryGroupOptions
			= new Dictionary<string, string>();
		ParseCategoryGroupOptions(ref categoryGroupOptions);

		//Create a dictionary of vendor filters
		Dictionary<string, string> vendorFilters
			= new Dictionary<string, string>();
		ParseVendorFilters(ref vendorFilters);

		//Determine specific filter placeholders 
		string filters = "";
		if ((categoryGroupOptions.Count > 0) 
		&& (vendorFilters.Count == 0)) {
			filters = ", _and_filters";
		} else if ((categoryGroupOptions.Count == 0) 
		&& (vendorFilters.Count > 0)) {
			filters = ", _or_filters";
		} else if ((categoryGroupOptions.Count > 0) 
		&& (vendorFilters.Count > 0)) {
			filters = ", _and_filters , _or_filters";
		}

		//Substitute the _filters placeholder for 0, 1 or 2 placeholders
		//depending on the value of @filters 
		Regex re_filters
			= new Regex("_filters",RegexOptions.Compiled);
		jsonQuery = re_filters.Replace(jsonQuery,filters);

		//Replace the _and_filters placeholder with AND clauses
		Regex re_and_filters
			= new Regex("_and_filters",RegexOptions.Compiled);
		jsonQuery = 
			re_and_filters.Replace(
			jsonQuery,ComposeAndFilters(categoryGroupOptions));

		//Replace the _or_filters placeholder with OR clauses
		Regex re_or_filters
			= new Regex("_or_filters",RegexOptions.Compiled);
		jsonQuery = 
			re_or_filters.Replace(
			jsonQuery,ComposeOrFilters(vendorFilters));

	}
	protected string ComposeSearchFields (
	Dictionary<string,string> searchFields ) {
		string result = "";
		int i = 0;
		foreach( KeyValuePair<string,string> kvp in searchFields) {
			result += "\"" + kvp.Value + "\"";
			if (i < searchFields.Count - 1) { result += ", "; }
			i++;
		}
		if (searchFields.Count == 0) {
			result = "\"_all\""; 
		}
		return result; 
	}
	protected string ComposeOrFilters (
	Dictionary<string,string> vendorFilters ) {
		string result = @"
{ 
	""query"" : {
		""filtered"" : {
			""query"" : {
				""match_all"" : { }
			},
			""filter"" : {
				""or"" : {
					""filters"" : [
" ;
		int i = 0;
		foreach( KeyValuePair<string,string> kvp in vendorFilters) {
			result += @" { ""term"" : { ""vendor.id"" : """ + kvp.Value 
				+ @""" } } ";
			if (i < vendorFilters.Count - 1) { result += ", "; }
			i++;
		}
		result += @" ] } } } } } ";
		return result;
	}
	protected string ComposeAndFilters ( 
	Dictionary<string,string> categoryGroupOptions ) {
		string result = "";
		int i = 0;
		foreach( KeyValuePair<string,string> kvp in categoryGroupOptions) {
			result += @"{ ""query"" : { ""query_string"" : { ""fields"" : [""option." 
				+ kvp.Key + @"""], ""query"" : """ + kvp.Value + @""" } } }";
			if (i < categoryGroupOptions.Count - 1) { result += ", "; } 
			i++;
		}
		return result;
	}
}
