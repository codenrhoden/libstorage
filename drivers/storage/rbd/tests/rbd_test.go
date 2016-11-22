package rbd

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/codedellemc/libstorage/api/server"
	apitests "github.com/codedellemc/libstorage/api/tests"

	// load the  driver
	"github.com/codedellemc/libstorage/drivers/storage/rbd"
	rbdx "github.com/codedellemc/libstorage/drivers/storage/rbd/executor"
)

var (
	configYAML = []byte(`
rbd:
  defaultPool: rbd
`)
)

func skipTests() bool {
	travis, _ := strconv.ParseBool(os.Getenv("TRAVIS"))
	noTest, _ := strconv.ParseBool(os.Getenv("TEST_SKIP_RBD"))
	return travis || noTest
}

func init() {
}

func TestMain(m *testing.M) {
	server.CloseOnAbort()
	ec := m.Run()
	os.Exit(ec)
}

func TestInstanceID(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	iid, err := rbdx.GetInstanceID()
	assert.NoError(t, err)
	if err != nil {
		t.Error("failed TestInstanceID")
		t.FailNow()
	}
	assert.NotEqual(t, iid, "")

	apitests.Run(
		t, rbd.Name, configYAML,
		(&apitests.InstanceIDTest{
			Driver:   rbd.Name,
			Expected: iid,
		}).Test)
}
