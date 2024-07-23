package main

import (
	"fmt"
	"os"
)

func main() {
	homedir, err := os.UserHomeDir()
	check(err)
	db := Data{
		Deals: importZohoDeals(fmt.Sprintf("%s/Downloads/Deals_2024_07_01.csv", homedir), fmt.Sprintf("%s/Downloads/Accounts_2024_07_22.csv", homedir), fmt.Sprintf("%s/Downloads/Contacts_2024_07_01.csv", homedir)),
		Leads: importZohoLeads(fmt.Sprintf("%s/Downloads/Leads_2024_07_01.csv", homedir)),
		Tasks: importZohoTasks(fmt.Sprintf("%s/Downloads/Tasks_2024_07_01.csv", homedir)),
	}
}
