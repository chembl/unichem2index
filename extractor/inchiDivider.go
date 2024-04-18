package extractor

import (
	"fmt"
	"go.uber.org/zap"
	"regexp"
	"strconv"
	"strings"
)

//InchiDivider the InChI on its different layers and components.
type InchiDivider struct {
	Logger *zap.SugaredLogger
}

func (ind *InchiDivider) ProcessInchi(compound Compound) (Compound, error) {
	logger := ind.Logger

	if len(compound.Inchi.Inchi) < 1 {
		var c Compound
		return c, fmt.Errorf("empty inchi for UCI %d", compound.UCI)
	}

	i := *new(Inchi)
	s, err := ind.splitInchi(compound.Inchi.Inchi)
	if err != nil {
		logger.Errorf("Error processing Inchi")
		return compound, err
	}
	i = Inchi{
		Version:               s["version"],
		Formula:               s["formula"],
		Connections:           s["connections"],
		HAtoms:                s["hAtoms"],
		Charge:                s["charge"],
		Protons:               s["protons"],
		StereoDbond:           s["stereoDbond"],
		StereoSP3:             s["stereoSP3"],
		StereoSP3inverted:     s["stereoSP3inverted"],
		StereoType:            s["stereoType"],
		IsotopicAtoms:         s["isotopicAtoms"],
		IsotopicExchangeableH: s["isotopicExchangeableH"],
		FullStereo:            fmt.Sprintf("%s%s%s%s", s["stereoDbond"], s["stereoSP3"], s["stereoSP3inverted"], s["stereoType"]),
		FullIsotopic:          fmt.Sprintf("%s%s", s["isotopicAtoms"], s["isotopicExchangeableH"]),
		Inchi:                 compound.Inchi.Inchi,
	}
	compound.Inchi = i
	compound.Components, err = ind.extractSubCompounds(i)
	logger.Debugw("Split Inchi", "UCI", compound.UCI, "Inchi", compound.Inchi)
	if err != nil {
		logger.Errorf("Error getting subcompounds ")
		return compound, err
	}

	return compound, nil
}

func (ind *InchiDivider) extractSubCompounds(inchi Inchi) ([]Inchi, error) {
	l := ind.Logger
	//inchis := new([]Inchi)

	formula, err := ind.splitFormulaLayer(inchi.Formula)
	if err != nil {
		return nil, err
	}

	if len(formula) <= 1 {
		return nil, nil
	}

	connection, err := ind.splitStandardLayer(inchi.Connections, len(formula))
	if err != nil {
		return nil, err
	}
	hAtoms, err := ind.splitStandardLayer(inchi.HAtoms, len(formula))
	if err != nil {
		return nil, err
	}
	charge, err := ind.splitStandardLayer(inchi.Charge, len(formula))
	if err != nil {
		return nil, err
	}
	stereoDbond, err := ind.splitStandardLayer(inchi.StereoDbond, len(formula))
	if err != nil {
		return nil, err
	}
	stereoSP3, err := ind.splitStandardLayer(inchi.StereoSP3, len(formula))
	if err != nil {
		return nil, err
	}
	isotopicAtoms, err := ind.splitStandardLayer(inchi.IsotopicAtoms, len(formula))
	if err != nil {
		return nil, err
	}
	inchis := make([]Inchi, len(formula))

	p := map[string][]string{
		"formula":       formula,
		"connection":    connection,
		"hAtoms":        hAtoms,
		"charge":        charge,
		"stereoDbond":   stereoDbond,
		"stereoSP3":     stereoSP3,
		"isotopicAtoms": isotopicAtoms,
	}

	for i := range inchis {
		inchis[i] = ind.buildInchi(p, i)
		inchis[i].Version = inchi.Version
		inchis[i].Protons = inchi.Protons
		if len(inchi.StereoSP3inverted) > 0 {
			if len(inchi.StereoSP3inverted) > i {
				inchis[i].StereoSP3inverted = string(inchi.StereoSP3inverted[i])
			} else {
				return nil, fmt.Errorf("more inchis found than stereo sp3 flag on StereoSP3inverted: %s", inchi)
			}
		}
		inchis[i].StereoType = inchi.StereoType
		inchis[i].IsotopicExchangeableH = inchi.IsotopicExchangeableH
		inchis[i].FullStereo = fmt.Sprintf("%s%s%s%s", inchis[i].StereoDbond, inchis[i].StereoSP3, inchis[i].StereoSP3inverted, inchi.StereoType)
		inchis[i].FullIsotopic = fmt.Sprintf("%s%s", inchis[i].IsotopicAtoms, inchis[i].IsotopicExchangeableH)
		inchis[i].buildInchiString()
		l.Debugf("Component inchi: %s", inchis[i].Inchi)
	}
	l.Debug("Inchi has %d components", len(inchis))

	return inchis, nil
}

func (ind *InchiDivider) buildInchi(layers map[string][]string, pos int) Inchi {
	i := Inchi{}
	for name, value := range layers {
		switch name {
		case "formula":
			if pos < len(value) {
				i.Formula = value[pos]
			} else {
				i.Formula = ""
			}
		case "connection":
			if pos < len(value) {
				i.Connections = value[pos]
			} else {
				i.Connections = ""
			}
		case "hAtoms":
			if pos < len(value) {
				i.HAtoms = value[pos]
			} else {
				i.HAtoms = ""
			}
		case "charge":
			if pos < len(value) {
				i.Charge = value[pos]
			} else {
				i.Charge = ""
			}
		case "stereoDbond":
			if pos < len(value) {
				i.StereoDbond = value[pos]
			} else {
				i.StereoDbond = ""
			}
		case "stereoSP3":
			if pos < len(value) {
				i.StereoSP3 = value[pos]
			} else {
				i.StereoSP3 = ""
			}
		case "isotopicAtoms":
			if pos < len(value) {
				i.IsotopicAtoms = value[pos]
			} else {
				i.IsotopicAtoms = ""
			}
		}
	}

	return i
}

func (ind *InchiDivider) splitFormulaLayer(layer string) ([]string, error) {
	r := regexp.MustCompile("^(\\d+)?(.*)$")
	return ind.splitLayer(layer, r, ".", 0)
}

func (ind *InchiDivider) splitStandardLayer(layer string, ninchi int) ([]string, error) {
	r := regexp.MustCompile("^(?:(\\d+)\\*)?(.*)$")
	return ind.splitLayer(layer, r, ";", ninchi)
}

func (ind *InchiDivider) splitLayer(layer string, reg *regexp.Regexp, separator string, ninchi int) ([]string, error) {
	log := ind.Logger
	if len(layer) < 1 {
		i := make([]string, ninchi)
		return i, nil
	}
	ls := strings.Split(layer, separator)
	inchiLayers := *new([]string)
	for _, l := range ls {
		p := reg.FindStringSubmatch(l)
		if p == nil {
			log.Errorf("Error trying to get number of components reg exp: %s layer: %s", reg.String(), l)
			return nil, fmt.Errorf("bad layer format: %s", l)
		}
		nmol := p[1]
		if len(nmol) < 1 {
			inchiLayers = append(inchiLayers, p[2])
		} else {
			a, err := strconv.Atoi(nmol)
			if err != nil {
				m := fmt.Sprintf("Split InChI error in ")
				fmt.Println(m)
				log.Error(m)
				return nil, err
			}
			for i := 0; i < a; i++ {
				inchiLayers = append(inchiLayers, p[2])
			}
		}
	}
	return inchiLayers, nil
}

func (ind *InchiDivider) splitInchi(inchi string) (map[string]string, error) {
	//logger := ex.Logger
	splitted := make(map[string]string)

	connectionsReg := `(?:/c(?P<connections>[^/]*))?`
	hAtomsReg := `(?:/h(?P<hAtoms>[^/]*))?`
	chargeReg := `(?:/q(?P<charge>[^/]*))?`
	protonsReg := `(?:/p(?P<protons>[^/]*))?`
	stereoDbondReg := `(?:/b(?P<stereoDbond>[^/]*))?`
	stereoSP3Reg := `(?:/t(?P<stereoSP3>[^/]*))?`
	stereoSP3invertedReg := `(?:/m(?P<stereoSP3inverted>[^/]*))?`
	stereoTypeReg := `(?:/s(?P<stereoType>\d))?`
	isotopicAtoms := `(?:/i(?P<isotopicAtoms>[^/]*))?`
	isotopicExchangeableH := `(?:/h(?P<isotopicExchangeableH>[^/]*))?`

	inchiReg := `^InChI=(?P<version>[^/]*)/(?P<formula>[^/]*)` + connectionsReg + hAtomsReg + chargeReg + protonsReg + stereoDbondReg + stereoSP3Reg + stereoSP3invertedReg + stereoTypeReg + isotopicAtoms + isotopicExchangeableH

	r := regexp.MustCompile(inchiReg)

	res := r.FindStringSubmatch(inchi)
	if res == nil {
		return splitted, fmt.Errorf("bad inchi format: %s", inchi)
	}

	for i, name := range r.SubexpNames() {
		if i != 0 {
			splitted[name] = res[i]
		}
	}
	//logger.Debug("Map", splitted)
	return splitted, nil
}
