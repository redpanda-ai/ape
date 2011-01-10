<%@ Page Language="C#" CodeFile="SearchTemplateTool.aspx.cs" 
    Inherits="SamplePage" AutoEventWireup="true" %>
<html>
<head runat="server" >
   <title>ElasticSearch / .NET Prototype</title>
</head>
<body bgcolor="DDDDDD">
	<form id="form1" runat="server">
		<br />
		<div>
		<table width=900>
			<tr>
				<td><b>Basic Search Options:</b> </td>
				<td> &nbsp; </td>
				<td> <B> Vendor Filters </B>
				<td> &nbsp; </td>
			</tr>
			<tr>
				<td>Search for: </td>
				<td>	<asp:TextBox id="SearchBox" runat="server" Text="" > 
					</asp:TextBox> </td>
				<td> Vendor ID 1: </td>
				<td> 	<asp:TextBox id="Vendor1" runat="server" Text="" > 
						</asp:TextBox> </td>
			</tr>
			<tr>
				<td>Country Code: </td>
				<td> <asp:TextBox id="CountryCode" runat="server" Text="" >
					</asp:TextBox> </td>
				<td> Vendor ID 2: </td>
				<td> 	<asp:TextBox id="Vendor2" runat="server" Text="" > 
						</asp:TextBox> </td>
			</tr>
			<tr>
				<td>Featured Item Date: </td>
				<td> <asp:TextBox id="FeaturedItemDate" runat="server" Text="" >
					</asp:TextBox> </td>
				<td> Vendor ID 3: </td>
				<td> 	<asp:TextBox id="Vendor3" runat="server" Text="" > 
						</asp:TextBox> </td>
			</tr>
			<tr>
				<td> Return Fields: 
				<td> <asp:TextBox id="ReturnFields" runat="server" Text="" >
					</asp:TextBox> </td>
				<td> &nbsp; </td>
				<td> &nbsp; </td>
			</tr>
			<tr>
				<td> &nbsp;
				<td>	<asp:Button id="SearchButton" runat="server" 
						onclick="SearchButton_Click" Text="Search" >
						</asp:Button> </td>
				<td> <B> Option Filters</B>
				<td> &nbsp;
			</tr>
			<tr>
				<td> <B> ElasticSearch </B> </td>
				<td> &nbsp; </td>
				<td> Reactivity: </td>
				<td> 	<asp:TextBox id="CGO_Reactivity" runat="server" Text="" >
					</asp:TextBox> </td>
			</tr>
			<tr>
				<td> Node Name:
				<td> 	<asp:TextBox id="NodeName" runat="server" Text="" > 
					</asp:TextBox> </td>
				<td> Antibody Products: </td>
				<td> 	<asp:TextBox id="CGO_AntibodyProducts" 
					runat="server" Text="" >
					</asp:TextBox> </td>
			</tr>
			<tr>
				<td> IndexName </td>
				<td> 	<asp:TextBox id="IndexName" runat="server" Text="" > 
						</asp:TextBox> </td>
				<td> Host Species: </td>
				<td> 	<asp:TextBox id="CGO_HostSpecies" runat="server" Text="" >
					</asp:TextBox> </td>
			</tr>
			<tr>
				<td> TypeName </td>
				<td> 	<asp:TextBox id="TypeName" runat="server" Text="" > 
						</asp:TextBox> </td>
				<td> Application: </td>
				<td> 	<asp:TextBox id="CGO_Application" runat="server" Text="" >
					</asp:TextBox> </td>
			</tr>
			<tr>
				<td><b>Paging and Sorting:</b> </td>
				<td> &nbsp; </td>
				<td> Conjugate: </td>
				<td> 	<asp:TextBox id="CGO_Conjugate" runat="server" Text="" >
					</asp:TextBox> </td>
			</tr>
			<tr>
				<td>Page Number: </td>
				<td> <asp:TextBox id="PageNumber" runat="server" Text="" >
					</asp:TextBox> </td>
				<td> <B> Search Field Filters</B>
				<td> &nbsp;
			</tr>
			<tr>
				<td>Page Size: </td>
				<td>	<asp:TextBox id="PageSize" runat="server" Text="" > 
					</asp:TextBox> </td>
				<td> Search Field 1:
				<td> 	<asp:TextBox id="SF1" runat="server" Text="" > 
					</asp:TextBox> </td>
			</tr>
			<tr>
				<td>Sort Field: </td>
				<td> <asp:TextBox id="SortField" runat="server" Text="" >
					</asp:TextBox> </td>
				<td> Search Field 2:
				<td> 	<asp:TextBox id="SF2" runat="server" Text="" > 
					</asp:TextBox> </td>
			</tr>
			<tr>
				<td>Sort Order: </td>
				<td> <asp:TextBox id="SortOrder" runat="server" Text="" >
					</asp:TextBox> </td>
				<td> Search Field 3:
				<td> 	<asp:TextBox id="SF3" runat="server" Text="" > 
					</asp:TextBox> </td>
			</tr>
	</table>

	<br> <b>Query produced:</b>
	<br> <asp:Label id="Query" runat="server" Text="" />
   <br> <b>Results: </b>
   <br>

      <asp:TextBox id="Results"
         runat="server" mode="multiline" 

	Width="80%" Height="80%" Wrap="true" TextMode="Multiline"
	Text="" >
      </asp:TextBox>
    </div>
  </form>
</body>
</html>
