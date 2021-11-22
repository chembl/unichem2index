package extractor

import (
	"context"
	"database/sql"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

type Source struct {
	SourceID         int       `bson:"sourceID,omitempty"`
	Name             string    `bson:"name,omitempty"`
	Description      string    `bson:"description,omitempty"`
	SrcReleaseNumber int32     `bson:"srcReleaseNumber,omitempty"`
	SrcReleaseDate   time.Time `bson:"srcReleaseDate,omitempty"`
	Created          time.Time `bson:"created,omitempty"`
	LastUpdated      time.Time `bson:"lastUpdated,omitempty"`
	LongName         string    `bson:"nameLong,omitempty"`
	SrcDetails       string    `bson:"srcDetails,omitempty"`
	SrcUrl           string    `bson:"srcUrl,omitempty"`
	BaseIdUrl        string    `bson:"baseIdUrl,omitempty"`
	Private          bool      `bson:"private,omitempty"`
	NameLabel        string    `bson:"nameLabel,omitempty"`
	UpdateComments   string    `bson:"updateComments,omitempty"`
	UCICount         int       `bson:"UCICount,omitempty"`
}

func getOriginalSources(ctx context.Context, l *zap.SugaredLogger, conf *Configuration) ([]Source, error) {
	db, err := sql.Open("godror", conf.OracleConn)
	if err != nil {
		m := fmt.Sprint("Go oracle open ERROR ")
		fmt.Println(m)
		l.Error(m)
		return nil, err
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			m := fmt.Sprint("Go oracle Closing DB ", err)
			fmt.Println(m)
			l.Error(m)
		}
	}(db)
	l.Info("Success connecting to Oracle DB")

	srcQuery := `
SELECT so.SRC_ID,
       so.NAME,
       so.DESCRIPTION,
       ur.SRC_RELEASE_NUMBER,
       ur.SRC_RELEASE_DATE,
       so.CREATED,
       so.LAST_UPDATED,
       so.NAME_LONG,
       so.SRC_DETAILS,
       so.SRC_URL,
       so.BASE_ID_URL,
       so.PRIVATE,
       so.NAME_LABEL,
       so.UPDATE_COMMENTS
FROM UC_SOURCE so,
     UC_RELEASE ur
WHERE so.CURRENT_RELEASE_U = ur.RELEASE_U
AND so.SRC_ID = ur.SRC_ID
ORDER BY so.SRC_ID`

	var (
		sourceID                                int
		srcReleaseNumber                        sql.NullInt32
		srcUrl, baseIdUrl, updateComments       sql.NullString
		name, description, longName, srcDetails string
		nameLabel                               string
		private                                 bool
		created, lastUpdated, srcReleaseDate    sql.NullTime
	)

	rows, err := db.QueryContext(ctx, srcQuery)
	if err != nil {
		m := fmt.Sprint("Failed to perform Oracle query")
		fmt.Println(m)
		l.Error(m)
		return nil, err
	}

	var sources []Source
	for rows.Next() {

		select {
		case <-ctx.Done():
			l.Warnf("Context done, ending OraDB sources load earlier")
		default:
			err := rows.Scan(
				&sourceID,
				&name,
				&description,
				&srcReleaseNumber,
				&srcReleaseDate,
				&created,
				&lastUpdated,
				&longName,
				&srcDetails,
				&srcUrl,
				&baseIdUrl,
				&private,
				&nameLabel,
				&updateComments)
			if err != nil {
				l.Error("Error reading line")
				return nil, err
			}
			l.Debugw(
				"Row:",
				"sourceID", sourceID,
				"name", name,
				"description", description,
				"srcReleaseNumber", srcReleaseNumber,
				"created", created,
				"lastUpdated", lastUpdated,
				"longName", longName,
				"baseIdUrl", baseIdUrl,
				"private", private,
			)

			var srn int32
			if srcReleaseNumber.Valid {
				srn = srcReleaseNumber.Int32
			}
			var srd time.Time
			if srcReleaseDate.Valid {
				srd = srcReleaseDate.Time
			}
			var cr time.Time
			if created.Valid {
				cr = created.Time
			}

			var lu time.Time
			if lastUpdated.Valid {
				lu = lastUpdated.Time
			}

			var su string
			if srcUrl.Valid {
				su = srcUrl.String
			}

			var biu string
			if baseIdUrl.Valid {
				biu = baseIdUrl.String
			}

			var uc string
			if updateComments.Valid {
				uc = updateComments.String
			}

			sources = append(sources, Source{
				SourceID:         sourceID,
				Name:             name,
				Description:      description,
				SrcReleaseNumber: srn,
				SrcReleaseDate:   srd,
				Created:          cr,
				LastUpdated:      lu,
				LongName:         longName,
				SrcDetails:       srcDetails,
				SrcUrl:           su,
				BaseIdUrl:        biu,
				Private:          private,
				NameLabel:        nameLabel,
				UpdateComments:   uc,
			})
			continue
		}
		break
	}

	return sources, nil
}

func fetchUCICounts(ctx context.Context, l *zap.SugaredLogger, conf *Configuration) (map[int]UCICount, error) {
	es := ElasticManager{
		Context:   ctx,
		IndexName: "unichem",
		TypeName:  "compound",
	}

	err := es.Init(ctx, conf, l)
	if err != nil {
		l.Error("Error init ElasticManager ", err)
		return nil, err
	}
	uc, err := es.getUCICountBySources()
	if err != nil {
		l.Error("Failed to get UCI Count by sources ")
		return nil, err
	}

	return uc, nil
}

func LoadSources(ctx context.Context, l *zap.SugaredLogger, conf *Configuration) error {

	UCICounts, err := fetchUCICounts(ctx, l, conf)
	if err != nil {
		m := fmt.Sprint("Failed to get UCI Count")
		fmt.Println(m)
		l.Error(m)
		return err
	}

	originalSources, err := getOriginalSources(ctx, l, conf)
	if err != nil {
		m := fmt.Sprint("Failed to getSources")
		fmt.Println(m)
		l.Error(m)
		return err
	}

	mc := conf.MongoDB
	co := options.Client().ApplyURI(mc)
	client, err := mongo.Connect(ctx, co)
	if err != nil {
		m := fmt.Sprint("Failed to connect to Mongo DB")
		fmt.Println(m)
		l.Error(m)
		return err
	}
	defer func(client *mongo.Client, ctx context.Context) {
		err := client.Disconnect(ctx)
		if err != nil {
			l.Panic("Failed to close MongoDB")
		}
	}(client, ctx)
	l.Debug("Connected to Mongo")

	sources := client.Database("ci_cache").Collection("source")

	o := options.Update().SetUpsert(true)
	for _, so := range originalSources {
		so.UCICount = UCICounts[so.SourceID].TotalUCI

		m := fmt.Sprint("Inserting: ", so.Name, " Private: ", so.Private, " ID: ", so.SourceID)
		l.Debug(m)
		up := bson.D{{"$set", so}}
		_, err = sources.UpdateByID(ctx, so.SourceID, up, o)
		if err != nil {
			m := fmt.Sprint("Failed to insert source: ", so.Name)
			fmt.Println(m)
			l.Error(m)
			return err
		}
	}
	m := fmt.Sprintf("%d Sources successfully added to de DB", len(originalSources))
	fmt.Println(m)
	l.Info(m)
	return nil
}
