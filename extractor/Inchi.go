package extractor

import "fmt"

// Inchi split in its components
type Inchi struct {
	Version               string `json:"version"`
	Formula               string `json:"formula"`
	Connections           string `json:"connections"`
	HAtoms                string `json:"h_atoms"`
	Charge                string `json:"charge"`
	Protons               string `json:"protons"`
	StereoDbond           string `json:"stereo_dbond"`
	StereoSP3             string `json:"stereo_SP3"`
	StereoSP3inverted     string `json:"stereo_SP3_inverted"`
	StereoType            string `json:"stereo_type"`
	IsotopicAtoms         string `json:"isotopic_atoms"`
	IsotopicExchangeableH string `json:"isotopic_exchangeable_h"`
	FullStereo            string `json:"full_stereo"`
	FullIsotopic          string `json:"full_isotopic"`
	Inchi                 string `json:"inchi"`
}

func (inchi *Inchi) buildInchiString() string {
	vf := fmt.Sprintf("InChI=%s/%s", inchi.Version, inchi.Formula)
	var connections, hAtoms, charge, protons, stereoDbond, stereoSP3, stereoSP3inverted, stereoType, isotopicAtoms, isotopicExchangeableH string

	if len(inchi.Connections) > 0 {
		connections = fmt.Sprintf("/c%s", inchi.Connections)
	}
	if len(inchi.HAtoms) > 0 {
		hAtoms = fmt.Sprintf("/h%s", inchi.HAtoms)
	}
	if len(inchi.Charge) > 0 {
		charge = fmt.Sprintf("/q%s", inchi.Charge)
	}
	if len(inchi.Protons) > 0 {
		protons = fmt.Sprintf("/p%s", inchi.Protons)
	}
	if len(inchi.StereoDbond) > 0 {
		stereoDbond = fmt.Sprintf("/b%s", inchi.StereoDbond)
	}
	if len(inchi.StereoSP3) > 0 {
		stereoSP3 = fmt.Sprintf("/t%s", inchi.StereoSP3)
	}
	if len(inchi.StereoSP3inverted) > 0 {
		stereoSP3inverted = fmt.Sprintf("/m%s", inchi.StereoSP3inverted)
	}
	if len(inchi.StereoType) > 0 {
		stereoType = fmt.Sprintf("/s%s", inchi.StereoType)
	}
	if len(inchi.IsotopicAtoms) > 0 {
		isotopicAtoms = fmt.Sprintf("/i%s", inchi.IsotopicAtoms)
	}
	if len(inchi.IsotopicExchangeableH) > 0 {
		isotopicExchangeableH = fmt.Sprintf("/h%s", inchi.IsotopicExchangeableH)
	}

	inchi.Inchi = vf + connections + hAtoms + charge + protons + stereoDbond + stereoSP3 + stereoSP3inverted + stereoType + isotopicAtoms + isotopicExchangeableH
	return inchi.Inchi
}
