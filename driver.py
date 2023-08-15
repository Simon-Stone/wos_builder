#!/usr/bin/env python
import os
import argparse
import logging
import xml.etree.cElementTree as ET
import read_records as rr
import extract as x
import db_info

log_levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def main(sourcefile, year, datadir, data_format):
    count = 0
    logging.debug("Starting processing {0}".format(sourcefile))

    Pubs_list = []
    Lang_list = []
    Head_list = []
    Subh_list = []
    Subj_list = []
    Publ_list = []
    Auth_list = []
    Inst_list = []
    NaIn_list = []
    Edit_list = []
    Refs_list = []
    Ftxt_list = []
    Fund_list = []
    Keyw_list = []
    Keyp_list = []
    Conf_list = []
    CoSp_list = []

    with open(args.sourcefile, "r") as data:
        while True:
            count += 1
            record = rr.get_record(data)

            if not record:
                logging.debug("Completed processing {0}".format(sourcefile))
                print("Processed {0} records".format(count - 1))
                break

            try:
                REC = ET.fromstring(record)
                wos_id = list(REC.iterfind("UID"))[0].text

                Pub, Languages, Headings, Subheadings, Subjects = x.extract_pub_info(
                    wos_id, REC
                )
                Pubs_list.extend(Pub)
                Lang_list.extend(Languages)
                Head_list.extend(Headings)
                Subh_list.extend(Subheadings)
                Subj_list.extend(Subjects)

                Publishers = x.extract_publisher(wos_id, REC)
                Publ_list.extend(Publishers)

                Authors = x.extract_authors(wos_id, REC)
                Auth_list.extend(Authors)

                Institutions, Name_inst_relation = x.extract_addresses(wos_id, REC)
                Inst_list.extend(Institutions)
                NaIn_list.extend(Name_inst_relation)

                Editions = x.extract_editions(wos_id, REC)
                Edit_list.extend(Editions)

                References = x.extract_references(wos_id, REC)
                Refs_list.extend(References)

                Ftext, Funding = x.extract_funding(wos_id, REC)
                Ftxt_list.extend(Ftext)
                Fund_list.extend(Funding)

                Conf, Sponsor = x.extract_conferences(wos_id, REC)
                Conf_list.extend(Conf)
                CoSp_list.extend(Sponsor)

                Keywords, Keywords_plus = x.extract_keywords(wos_id, REC)
                Keyw_list.extend(Keywords)
                Keyp_list.extend(Keywords_plus)

            except Exception as e:
                print("[ERROR:{0}] Caught an exception : {1}".format(wos_id, e))
                pass

    try:
        x.dump(
            Pubs_list,
            db_info.h_source,
            db_info.t_source,
            "source",
            "{0}/source.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Edit_list,
            db_info.h_editions,
            db_info.t_editions,
            "editions",
            "{0}/editions.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Ftxt_list,
            db_info.h_fundingtexts,
            db_info.t_fundingtexts,
            "fundingtext",
            "{0}/fundingtext.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Fund_list,
            db_info.h_funding,
            db_info.t_funding,
            "funding",
            "{0}/funding.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Keyw_list,
            db_info.h_keywords,
            db_info.t_keywords,
            "keywords",
            "{0}/keywords.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Keyp_list,
            db_info.h_keywords_plus,
            db_info.t_keywords_plus,
            "keywords_plus",
            "{0}/keywords_plus.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Conf_list,
            db_info.h_conferences,
            db_info.t_conferences,
            "conferences",
            "{0}/conferences.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            CoSp_list,
            db_info.h_conf_sponsors,
            db_info.t_conf_sponsors,
            "confSponsors",
            "{0}/confSponsors.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Refs_list,
            db_info.h_references,
            db_info.t_references,
            "refs",
            "{0}/references.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Pubs_list,
            db_info.h_publications,
            db_info.t_publications,
            "publications",
            "{0}/publications.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Lang_list,
            db_info.h_languages,
            db_info.t_languages,
            "languages",
            "{0}/langauges.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Head_list,
            db_info.h_headings,
            db_info.t_headings,
            "headings",
            "{0}/headings.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Subh_list,
            db_info.h_subheadings,
            db_info.t_subheadings,
            "subheadings",
            "{0}/subheadings.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Subj_list,
            db_info.h_subjects,
            db_info.t_subjects,
            "subjects",
            "{0}/subjects.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Publ_list,
            db_info.h_publishers,
            db_info.t_publishers,
            "publishers",
            "{0}/publishers.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Auth_list,
            db_info.h_contributors,
            db_info.t_contributors,
            "contributors",
            "{0}/contributors.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            Inst_list,
            db_info.h_institutions,
            db_info.t_institutions,
            "institutions",
            "{0}/institutions.{1}".format(datadir, data_format),
            data_format=data_format,
        )
        x.dump(
            NaIn_list,
            db_info.h_name_inst,
            db_info.t_name_inst,
            "affiliations",
            "{0}/affiliations.{1}".format(datadir, data_format),
            data_format=data_format,
        )

    except Exception:
        print("[ERROR] Dumping failed for {0}".format(sourcefile))
        logging.error("[ERROR] Dumping failed for {0}".format(sourcefile))
        exit(-1)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--sourcefile", default="sample.xml", help="Path to data file"
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        default="DEBUG",
        help="set level of verbosity, DEBUG, INFO, WARN",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        default="./extract.log",
        help="Logfile path. Defaults to ./tabulator.log",
    )
    parser.add_argument(
        "-d",
        "--dir",
        default=".",
        help="Folder to write data to, Default is current folder",
    )
    parser.add_argument(
        "-f", "--format", default="sql", help="Output format to dump into"
    )
    args = parser.parse_args()

    print("Processing : {0}".format(args.sourcefile))

    logging.basicConfig(
        filename=args.logfile,
        level=log_levels[args.verbosity],
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
    )

    logging.debug("Document processing starts")

    if not os.path.exists(args.dir):
        os.makedirs(args.dir)
    logging.debug("Data output folder confirmed  :  {0}".format(args.dir))

    year = ""
    if os.path.basename(args.sourcefile).startswith("WR"):
        s = args.sourcefile.split("_")
        year = s[1] + "_"
        print(args.sourcefile.replace(".xml", ""))

    print("[DEBUG] Processing year : {0}".format(year))

    main(args.sourcefile, year, args.dir, args.format)
