package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgraph-io/badger/v4"
)

type Contacts struct{ Contacts []Contact }
type Leads struct{ Leads []Lead }
type Deals struct{ Deals []Deal }
type Tasks struct{ Tasks []Task }
type Users struct{ Users []User }

type DB struct {
	db *badger.DB
}

type DataLake struct {
	Leads   Leads
	Deals   Deals
	Tasks   Tasks
	Users   Users
	LeadsDB *DB
	DealsDB *DB
	TasksDB *DB
	UsersDB *DB
}

type Address struct {
	Street1     string
	Street2     string
	City        string
	State       string
	Zip         string
	CountryCode int
}

type Company struct {
	Name          string
	Website       string
	AnnualRevenue string
	SicCode       string
	Physical      Address
	Mailing       Address
}

type Contact struct {
	Id             string
	OwnerId        string
	FirstName      string
	LastName       string
	Email          string
	PhoneNumber    string
	AltPhoneNumber string
	Tags           []string
	Company        Company
}

type Lead struct {
	Status  string
	Contact Contact
}

type Deal struct {
	Stage   Stage
	Contact Contact
}

type User struct {
	Id    string
	Name  string
	Email string
}

type Task struct {
	Id          string
	OwnerId     string
	Subject     string
	DueDate     string
	RelatedTo   string
	RelatedToId string
	Status      string
	Priority    string
	Description string
	Tags        []string
}

type Stage struct {
	Name        string
	Probability string
}

func check(err error) {
	if err != nil {
		log.Fatalf(err.Error())
	}
}

func homedir() string {
	homedir, err := os.UserHomeDir()
	check(err)
	return homedir
}

/* import - zoho */

// import zoho deals from CSV
func (d *DataLake) importZohoDeals(pathToDealsCSV, pathToAccountsCSV, pathToContactsCSV string) {
	dealFile, err := os.Open(pathToDealsCSV)
	check(err)
	defer dealFile.Close()
	accountFile, err := os.Open(pathToAccountsCSV)
	check(err)
	defer accountFile.Close()
	contactFile, err := os.Open(pathToContactsCSV)
	check(err)
	defer contactFile.Close()
	dealLines, err := csv.NewReader(dealFile).ReadAll()
	check(err)
	accountLines, err := csv.NewReader(accountFile).ReadAll()
	check(err)
	contactLines, err := csv.NewReader(contactFile).ReadAll()
	check(err)
	// TODO: programmatic indexing
	for _, x := range dealLines[1:] {
		stage := Stage{
			Name:        x[7],
			Probability: x[9],
		}
		var company Company
		for _, y := range accountLines[1:] {
			if y[0] == x[5] {
				company = Company{
					Name:          y[3],
					Website:       y[8],
					AnnualRevenue: y[12],
					SicCode:       y[13],
				}
			}
		}
		var contact Contact
		for _, z := range contactLines[1:] {
			var user User
			t := false
			for _, a := range d.Users.Users {
				if a.Id == z[1] {
					t = true
				}
			}
			if !t {
				user.Id = z[1]
				user.Name = z[2]
				d.Users.Users = append(d.Users.Users, user)
			}
			if z[0] == x[15] {
				contact = Contact{
					Id:             z[0],
					OwnerId:        z[1],
					FirstName:      z[3],
					LastName:       z[4],
					Email:          z[9],
					PhoneNumber:    z[12],
					AltPhoneNumber: z[13],
					Tags:           strings.Split(z[38], ","),
					Company:        company,
				}
			}
		}
		deal := Deal{
			Stage:   stage,
			Contact: contact,
		}
		d.Deals.Deals = append(d.Deals.Deals, deal)
		// broken
		val, err := d.DealsDB.Get([]byte(deal.Contact.Id))
		check(err)
		var buf bytes.Buffer
		if len(val) == 0 {
			gob.NewEncoder(&buf).Encode(deal)
			d.DealsDB.Set([]byte(deal.Contact.Id), buf.Bytes())
		}
	}
}

// import zoho leads from CSV
func (d *DataLake) importZohoLeads(path string) {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	check(err)
	// TODO: programmatic indexing
	for _, x := range lines[1:] {
		company := Company{
			Name:          x[3],
			Website:       x[8],
			AnnualRevenue: x[10],
			Physical: Address{
				Street1: x[12],
				City:    x[13],
				State:   x[14],
				Zip:     x[15],
			},
			Mailing: Address{
				Street1: x[41],
				City:    x[42],
				State:   x[38],
				Zip:     x[37],
			},
		}
		lead := Lead{Status: x[9], Contact: Contact{
			Company:        company,
			Id:             x[0],
			OwnerId:        x[1],
			FirstName:      x[4],
			LastName:       x[5],
			Email:          x[6],
			PhoneNumber:    x[7],
			AltPhoneNumber: x[32],
			Tags:           strings.Split(x[19], ","),
		}}
		d.Leads.Leads = append(d.Leads.Leads, lead)
		val, err := d.LeadsDB.Get([]byte(lead.Contact.Id))
		check(err)
		var buf bytes.Buffer
		if len(val) == 0 {
			gob.NewEncoder(&buf).Encode(lead)
			d.LeadsDB.Set([]byte(lead.Contact.Id), buf.Bytes())
		}
	}
}

// import zoho tasks from CSV
func (d *DataLake) importZohoTasks(path string) {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	check(err)
	// TODO: programmatic indexing
	for _, x := range lines[1:] {
		task := Task{
			Id:          x[0],
			OwnerId:     x[1],
			Subject:     x[3],
			DueDate:     x[4],
			RelatedTo:   x[8],
			RelatedToId: x[7],
			Status:      x[9],
			Priority:    x[10],
			Description: x[17],
			Tags:        strings.Split(x[21], ","),
		}
		d.Tasks.Tasks = append(d.Tasks.Tasks, task)
		_, err := d.LeadsDB.Get([]byte(task.Id))
		var buf bytes.Buffer
		if err != nil {
			gob.NewEncoder(&buf).Encode(task)
			d.LeadsDB.Set([]byte(task.Id), buf.Bytes())
		}
	}
}

/* operations */

func (d *Deals) addDeal(id, ownerid, firstname, lastname, email, phonenumber string, company Company, tags ...string) {
	d.Deals = append(d.Deals, Deal{Contact: Contact{
		Id:          id,
		OwnerId:     ownerid,
		FirstName:   firstname,
		LastName:    lastname,
		Email:       email,
		PhoneNumber: phonenumber,
		Tags:        tags,
		Company:     company,
	}})
}

func (l *Leads) addLead(id, ownerid, firstname, lastname, email, phonenumber string, company Company, tags ...string) {
	l.Leads = append(l.Leads, Lead{Contact: Contact{
		Id:          id,
		OwnerId:     ownerid,
		FirstName:   firstname,
		LastName:    lastname,
		Email:       email,
		PhoneNumber: phonenumber,
		Tags:        tags,
		Company:     company,
	}})
}

func (t *Tasks) addTask(id, ownerid, subject, duedate, relatedto, relatedtoid, status, priority, description string, tags ...string) {
	t.Tasks = append(t.Tasks, Task{
		Id:          id,
		OwnerId:     ownerid,
		Subject:     subject,
		DueDate:     duedate,
		RelatedTo:   relatedto,
		RelatedToId: relatedtoid,
		Status:      status,
		Priority:    priority,
		Description: description,
		Tags:        tags,
	})
}

// convert Leads to Contacts
func (l Leads) toContacts() Contacts {
	var contacts Contacts
	for _, x := range l.Leads {
		contacts.Contacts = append(contacts.Contacts, x.Contact)
	}
	return contacts
}

// convert Deals to Contacts
func (d Deals) toContacts() Contacts {
	var contacts Contacts
	for _, x := range d.Deals {
		contacts.Contacts = append(contacts.Contacts, x.Contact)
	}
	return contacts
}

// filter Leads, return Contacts with a tag matching one of the given "tags"
func (c Contacts) filterByTags(tags ...string) Contacts {
	var contacts Contacts
	for _, x := range c.Contacts {
		t := false
		for _, y := range x.Tags {
			for _, z := range tags {
				if y == z {
					t = true
				}
			}
		}
		if t {
			contacts.Contacts = append(contacts.Contacts, x)
		}
	}
	return contacts
}

// filter Leads, return Contacts with an email address
func (c Contacts) filterByHasEmail() Contacts {
	var contacts Contacts
	for _, x := range c.Contacts {
		if x.Email != "" {
			contacts.Contacts = append(contacts.Contacts, x)
		}
	}
	return contacts
}

// filter Leads, return Contacts matching lead owner ID
func (c Contacts) filterByOwnerID(id string) Contacts {
	var contacts Contacts
	for _, x := range c.Contacts {
		if x.OwnerId == id {
			contacts.Contacts = append(contacts.Contacts, x)
		}
	}
	return contacts
}

// WIP: this is broken
// return Deal with given id from badgerdb
func (d *DataLake) getDealById(id string) Deal {
	var deal Deal
	check(d.DealsDB.db.View(func(txn *badger.Txn) error {
		i, err := txn.Get([]byte(id))
		if err != nil {
			return nil
		}
		val, err := i.ValueCopy(nil)
		if err != nil {
			return nil
		}
		d := gob.NewDecoder(bytes.NewReader(val))
		if err := d.Decode(&deal); err != nil {
			panic(err)
		}
		return nil
	}))
	return deal
}

// return Lead with given id from badgerdb
func (d *DataLake) getLeadById(id string) Lead {
	var lead Lead
	check(d.LeadsDB.db.View(func(txn *badger.Txn) error {
		i, err := txn.Get([]byte(id))
		if err != nil {
			return nil
		}
		val, err := i.ValueCopy(nil)
		if err != nil {
			return nil
		}
		d := gob.NewDecoder(bytes.NewReader(val))
		if err := d.Decode(&lead); err != nil {
			panic(err)
		}
		return nil
	}))
	return lead
}

/* badger */

// initiate 4 badger dbs and nest them in a DataLake{} - /tmp/leads, /tmp/deals, /tmp/tasks, /tmp/users
func Initiate() DataLake {
	db := DataLake{}
	var err error
	db.LeadsDB = Open("/tmp/leads")
	check(err)
	db.DealsDB = Open("/tmp/deals")
	check(err)
	db.TasksDB = Open("/tmp/tasks")
	check(err)
	db.UsersDB = Open("/tmp/users")
	check(err)
	return db
}

// open badgerdb located at pathToDb
func Open(pathToDb string) *DB {
	opts := badger.DefaultOptions(pathToDb)
	opts.Logger = nil
	b, err := badger.Open(opts)
	check(err)
	return &DB{db: b}
}

// wrapper for *badger.DB.Close()
func (d *DB) Close() error {
	return d.db.Close()
}

// set badgerdb record
func (db *DB) Set(k, v []byte) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(k, v)
		return err
	})
	return err
}

// get badgerdb record
func (db *DB) Get(k []byte) ([]byte, error) {
	var b []byte
	err := db.db.View(func(txn *badger.Txn) error {
		i, err := txn.Get(k)
		if err != nil {
			return nil
		}
		check(err)
		b, err = i.ValueCopy(b)
		return err
	})
	return b, err
}

// delete badgerdb record
func (db *DB) Delete(k []byte) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(k)
		return err
	})
	return err
}

// TODO
func (d *DataLake) syncLocal() error { return fmt.Errorf("hello, world") }

/* export - CSV */

// export []Contact to "/export.csv"
func (c Contacts) exportToCSV() {
	f, err := os.Create("export.csv")
	check(err)
	defer f.Close()
	// TODO: programmatic indexing
	index := []string{"Id", "OwnerId", "FirstName", "LastName", "Email", "PhoneNumber", "AltPhoneNumber", "Tags", "Company Name", "Website", "Annual Revenue", "Sic Code", "Addresses"}
	w := csv.NewWriter(f)
	w.Write(index)
	for _, x := range c.Contacts {
		w.Write(append([]string{}, x.Id, x.OwnerId, x.FirstName, x.LastName, x.Email, x.PhoneNumber, x.AltPhoneNumber, toCsv(x.Tags), x.Company.Name, x.Company.Website, x.Company.AnnualRevenue, x.Company.SicCode, x.Company.Physical.Street1, x.Company.Physical.Street2, x.Company.Physical.City, x.Company.Physical.State, x.Company.Physical.Zip, fmt.Sprintf("%d", x.Company.Physical.CountryCode), x.Company.Mailing.Street1, x.Company.Mailing.Street2, x.Company.Mailing.City, x.Company.Mailing.State, x.Company.Mailing.Zip, fmt.Sprintf("%d", x.Company.Mailing.CountryCode)))
	}
	w.Flush()
}

func toCsv(s []string) string {
	var returned string
	for _, x := range s {
		returned += x + ","
	}
	return returned[:len(returned)-1]
}

/* export - s3 */

// upload deals to s3 bucket
func (d DataLake) uploadDealsToS3(bucketName, regionName string) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(regionName))
	check(err)
	client := s3.NewFromConfig(cfg)
	var buf bytes.Buffer
	for _, x := range d.Deals.Deals {
		gob.NewEncoder(&buf).Encode(x)
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(x.Contact.Id),
			Body:   bytes.NewReader(buf.Bytes()),
		})
		check(err)
	}
}

// upload leads to s3 bucket
func (d DataLake) uploadLeadsToS3(bucketName, regionName string) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(regionName))
	check(err)
	client := s3.NewFromConfig(cfg)
	var buf bytes.Buffer
	for _, x := range d.Leads.Leads {
		gob.NewEncoder(&buf).Encode(x)
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(x.Contact.Id),
			Body:   bytes.NewReader(buf.Bytes()),
		})
		check(err)
	}
}
