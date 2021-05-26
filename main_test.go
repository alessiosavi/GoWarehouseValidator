package main

import (
	"github.com/alessiosavi/GoWarehouseValidator/datastructure"
	"testing"
)

func Test_core(t *testing.T) {
	type args struct {
		conf *datastructure.Conf
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
		{
			name: "",
			args: args{
				conf: &datastructure.Conf{
					Path: []string{"C:\\opt\\SP\\worksapce\\Jupyter\\thom-browne-timeseries\\database\\CL1_SALESEXPORT.csv"},
					Validation: map[string]string{"GL_DATEPIECE": "DATE",
						"GL_ETABLISSEMENT": "INTEGER",
						"GL_CODEARTICLE":   "STRING",
						"GL_QTEFACT":       "FLOAT",
						"GL_REFARTBARRE":   "STRING",
						"GL_DEVISE":        "STRING",
						"GL_TOTALHT":       "FLOAT",
						"DISCOUNTFLAG":     "STRING|NULLABLE"},
					Separator:  "|",
					DateFormat: "%Y-%m-%d %H:%M:%S",
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := tt.args.conf.NewValidator()
			if got := core(validator); got != tt.want {
				t.Errorf("core() = %v, want %v", got, tt.want)
			}
		})
	}
}
