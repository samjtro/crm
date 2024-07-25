package main

import (
	"fmt"
)

func main() {
	db := Initiate()
	defer db.LeadsDB.Close()
	defer db.DealsDB.Close()
	defer db.TasksDB.Close()
	defer db.UsersDB.Close()
	db.importZohoDeals(fmt.Sprintf("%s/Downloads/Deals_2024_07_01.csv", homedir()), fmt.Sprintf("%s/Downloads/Accounts_2024_07_22.csv", homedir()), fmt.Sprintf("%s/Downloads/Contacts_2024_07_01.csv", homedir()))
	db.importZohoLeads(fmt.Sprintf("%s/Downloads/Leads_2024_07_01.csv", homedir()))
	db.importZohoTasks(fmt.Sprintf("%s/Downloads/Tasks_2024_07_01.csv", homedir()))
}
