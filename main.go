package main

import (
	"fmt"
	"os"
)

func main() {
	homedir, err := os.UserHomeDir()
	check(err)
	importZohoDeals(fmt.Sprintf("%s/Downloads/Deals_2024_07_01.csv", homedir), fmt.Sprintf("%s/Downloads/Accounts_2024_07_22.csv", homedir), fmt.Sprintf("%s/Downloads/Contacts_2024_07_01.csv", homedir))
	//importZohoLeads(fmt.Sprintf("%s/Downloads/Leads_2024_07_22.csv", homedir))
	//importZohoTasks(fmt.Sprintf("%s/Downloads/Tasks_2024_07_22.csv", homedir))
}
