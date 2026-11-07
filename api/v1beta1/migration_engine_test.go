package v1beta1_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

const (
	legacyEngine = "pgcopydb"
	nativeEngine = "rpgmigrate"
)

func TestMigrationEngineRoundTrip(t *testing.T) {
	for _, engine := range []string{"", legacyEngine, nativeEngine} {
		for _, version := range []string{"v1alpha1", "v1beta1"} {
			t.Run(version+"/"+engine, func(t *testing.T) {
				var object runtime.Object = &v1alpha1.Migration{}
				var other runtime.Object = &v1beta1.Migration{}
				if version == "v1beta1" {
					object, other = other, object
				}
				input := map[string]any{"spec": engineSpec(engine)}
				raw, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				for _, target := range []runtime.Object{object, other, object} {
					if err := json.Unmarshal(raw, target); err != nil {
						t.Fatal(err)
					}
					raw, err = json.Marshal(target.DeepCopyObject())
					if err != nil {
						t.Fatal(err)
					}
					var got map[string]any
					if err := json.Unmarshal(raw, &got); err != nil {
						t.Fatal(err)
					}
					value, present := got["spec"].(map[string]any)["engine"]
					if present != (engine != "") || (present && value != engine) {
						t.Fatalf("%T engine = %v (present %t), want %q", target, value, present, engine)
					}
				}
			})
		}
	}
}

func TestMigrationEngineSchema(t *testing.T) {
	raw, err := os.ReadFile("../../config/crd/bases/pgcopydb-operator.io_migrations.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(raw, &crd); err != nil {
		t.Fatal(err)
	}
	if crd.Spec.Conversion != nil && crd.Spec.Conversion.Strategy != apiextensionsv1.NoneConverter {
		t.Fatal("engine round trips require conversion strategy None")
	}
	if len(crd.Spec.Versions) != 2 {
		t.Fatalf("CRD has %d versions, want 2", len(crd.Spec.Versions))
	}
	for _, version := range crd.Spec.Versions {
		t.Run(version.Name, func(t *testing.T) {
			if !version.Served || version.Schema == nil {
				t.Fatal("missing served version schema")
			}
			props := version.Schema.OpenAPIV3Schema.Properties["spec"]
			if !reflect.DeepEqual(props, crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]) {
				t.Fatal("served spec schemas differ under conversion strategy None")
			}
			var internal apiextensions.JSONSchemaProps
			if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&props, &internal, nil); err != nil {
				t.Fatal(err)
			}
			schema, err := structuralschema.NewStructural(&internal)
			if err != nil {
				t.Fatal(err)
			}
			openAPI, _, err := validation.NewSchemaValidator(&internal)
			if err != nil {
				t.Fatal(err)
			}
			celValidator := cel.NewValidator(schema, false, celconfig.PerCallLimit)
			if celValidator == nil {
				t.Fatal("schema has no CEL validator")
			}
			for _, tc := range []struct {
				name   string
				engine any
				want   any
				valid  bool
			}{
				{"omitted", nil, legacyEngine, true},
				{"legacy", legacyEngine, legacyEngine, true},
				{"native", nativeEngine, nativeEngine, true},
				{"unknown", "invalid", "invalid", false},
				{"case", "PGCOPYDB", "PGCOPYDB", false},
				{"empty", "", "", false},
				{"number", float64(1), float64(1), false},
			} {
				t.Run("create/"+tc.name, func(t *testing.T) {
					spec := engineSpec("")
					if tc.engine != nil {
						spec["engine"] = tc.engine
					}
					defaulting.Default(spec, schema)
					if spec["engine"] != tc.want {
						t.Fatalf("defaulted engine = %v, want %v", spec["engine"], tc.want)
					}
					errs := validation.ValidateCustomResource(field.NewPath("spec"), spec, openAPI)
					celErrors, _ := celValidator.Validate(t.Context(), field.NewPath("spec"), schema, spec, nil, celconfig.RuntimeCELCostBudget)
					errs = append(errs, celErrors...)
					if tc.valid != (len(errs) == 0) {
						t.Fatalf("create errors = %v, valid = %t", errs, tc.valid)
					}
				})
			}
			for _, oldEngine := range []string{"", legacyEngine, nativeEngine} {
				for _, newEngine := range []string{"", legacyEngine, nativeEngine} {
					for _, withDefaults := range []bool{false, true} {
						name := oldEngine + "->" + newEngine + "/raw"
						if withDefaults {
							name = oldEngine + "->" + newEngine + "/defaulted"
						}
						t.Run(name, func(t *testing.T) {
							oldSpec, newSpec := engineSpec(oldEngine), engineSpec(newEngine)
							if withDefaults {
								defaulting.Default(oldSpec, schema)
								defaulting.Default(newSpec, schema)
								newSpec["suspend"] = true
							}
							errs, _ := celValidator.Validate(t.Context(), field.NewPath("spec"), schema, newSpec, oldSpec, celconfig.RuntimeCELCostBudget)
							allowed := (oldEngine == nativeEngine) == (newEngine == nativeEngine)
							if allowed != (len(errs) == 0) {
								t.Fatalf("update errors = %v, allowed = %t", errs, allowed)
							}
							if !allowed && !strings.Contains(errs.ToAggregate().Error(), "engine is immutable") {
								t.Fatalf("update rejected for wrong reason: %v", errs)
							}
						})
					}
				}
			}
		})
	}
}

func engineSpec(engine string) map[string]any {
	spec := map[string]any{
		"source": map[string]any{"host": "source", "username": "migration"},
		"target": map[string]any{"host": "target", "username": "migration"},
	}
	if engine != "" {
		spec["engine"] = engine
	}
	return spec
}
