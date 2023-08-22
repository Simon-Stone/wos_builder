#!/usr/bin/env python
# coding: utf-8
import argparse

# import mysql.connector
import logging
import xml.etree.cElementTree as ET
import json

log_levels = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def load_data(datafile):
    context = ET.iterparse(datafile, events=("start", "end"))
    logging.debug("Got context")
    context = iter(context)
    return context


# uid -> wos_id, citedAuthor, year , page, volume, citedTitle, citedWork, doi
def extract_references(wos_id, elem):
    references = []
    for ref in list(
        elem.iterfind("./static_data/fullrecord_metadata/references/reference")
    ):
        cur = {"wos_id": wos_id, "citedId": ref.find("uid").text}

        references.append(cur)

    return references


def extract_editions(wos_id, elem):
    return [
        {"wos_id": wos_id, "edition": i.attrib["value"]}
        for i in elem.iterfind("./static_data/summary/EWUID/edition")
    ]


def extract_addresses(wos_id, elem):
    addresslist = []
    name_address_relation = []

    for addresses in elem.iterfind(
        "./static_data/fullrecord_metadata/addresses/address_name"
    ):
        # print "-"*50
        addr = {
            "wos_id": wos_id,
            "addr_num": list(addresses.iterfind("./address_spec"))[0].attrib["addr_no"],
            "organization": "NULL",
        }

        for address in addresses.iter():
            if address.tag in ["full_address", "city", "state", "country", "zip"]:
                # print address.tag
                addr[str(address.tag)] = str(address.text)

        orgs = []
        for item in addresses.iter():
            if item.tag == "organization":
                orgs.extend([item.text])
        if not orgs:
            orgs = ["NULL"]
        # print "Organizations : ", orgs

        suborgs = []
        for item in addresses.iter():
            if item.tag == "suborganization":
                suborgs.extend([item.text])
        if not suborgs:
            suborgs = ["NULL"]

        # print "SubOrganizations : ", suborgs
        for org in orgs:
            for suborg in suborgs:
                t = {"organization": org, "suborganization": suborg}
                temp = addr.copy()
                temp.update(t)
                addresslist.extend([temp])

        # print addresslist
        for name in list(addresses.iterfind("./names/name")):
            # print "Name: ", name.tag, name.attrib
            # print "{0} {1} {2}".format(wos_id, name.attrib['seq_no'], name.attrib['addr_no'])
            name_address_relation.extend(
                [
                    {
                        "wos_id": wos_id,
                        "position": name.attrib["seq_no"],
                        "addr_num": name.attrib["addr_no"],
                    }
                ]
            )
    # print addresslist
    return addresslist, name_address_relation


def extract_authors(wos_id, elem):
    authors = []

    for names in elem.iterfind("./static_data/summary/names"):
        for name in names:
            author = {
                "wos_id": wos_id,
                "position": name.attrib.get("seq_no", "NULL"),
                "reprint": name.attrib.get("reprint", "NULL"),
                "cluster_id": name.attrib.get("dais_id", "NULL"),
                "role": name.attrib.get("role", "NULL"),
            }
            for item in name.iter():
                author[str(item.tag)] = str(item.text)
            authors.extend([author])

    return authors


def extract_publisher(wos_id, elem):
    publisher = {"wos_id": wos_id}

    for publishers in elem.iterfind("./static_data/summary/publishers"):
        for item in publishers.iter():
            if item.tag in ["display_name", "full_name", "full_address", "city"]:
                publisher[item.tag] = item.text

    return [publisher]


def extract_conferences(wos_id, elem):
    conferences = []
    sponsors = []

    for conf in list(elem.iterfind("./static_data/summary/conferences/conference")):
        # Do try catch on each of these

        conference = {"wos_id": wos_id}
        conference["conf_id"] = conf.attrib.get("conf_id", "NULL")

        try:
            conference["info"] = list(conf.iterfind("./conf_infos/conf_info"))[0].text
        except Exception:
            conference["info"] = "NULL"
        try:
            conference["title"] = list(conf.iterfind("./conf_titles/conf_title"))[
                0
            ].text
        except Exception:
            conference["title"] = "NULL"
        try:
            conference["dates"] = list(conf.iterfind("./conf_dates/conf_dates"))[0].text
        except Exception:
            conference["dates"] = "NULL"
        try:
            conference.update(list(conf.iterfind("./conf_dates/conf_date"))[0].attrib)
        except Exception:
            pass
        try:
            conference["conf_city"] = list(
                conf.iterfind("./conf_locations/conf_location/conf_city")
            )[0].text
        except Exception:
            conference["conf_city"] = "NULL"
        try:
            conference["conf_state"] = list(
                conf.iterfind("./conf_locations/conf_location/conf_state")
            )[0].text
        except Exception:
            conference["conf_state"] = "NULL"
        try:
            conference["conf_host"] = list(
                conf.iterfind("./conf_locations/conf_location/conf_host")
            )[0].text
        except Exception:
            conference["conf_host"] = "NULL"

        for sponsor in list(conf.iterfind("./sponsors/sponsor")):
            # print "Sponsor {0}/{1}: {2}".format(wos_id, conference['conf_id'], sponsor.text)
            sponsors.extend(
                [
                    {
                        "wos_id": wos_id,
                        "conf_id": conference["conf_id"],
                        "sponsor": sponsor.text,
                    }
                ]
            )

        conferences.extend([conference])
    return conferences, sponsors


def extract_funding(wos_id, elem):
    funding = []
    text = "NULL"
    for t in list(
        elem.iterfind("./static_data/fullrecord_metadata/fund_ack/fund_text")
    ):
        for para in t.iter():
            if text == "NULL":
                text = ""
            text = text + str(para.text) + "\n"

    for g in list(
        elem.iterfind("./static_data/fullrecord_metadata/fund_ack/grants/grant")
    ):
        grant_agency = None
        for agency in g.iterfind("./grant_agency"):
            grant_agency = agency.text

        grant_id_list = []
        for grant_id in g.iterfind("./grant_ids/grant_id"):
            grant_id_list.extend([str(grant_id.text)])

        if not grant_id_list:
            grant_id_list = ["NULL"]

        for g in grant_id_list:
            funding.extend([{"wos_id": wos_id, "agency": grant_agency, "grant_id": g}])

    fundingText = []
    if text != "NULL":
        fundingText = [{"wos_id": wos_id, "funding_text": text}]
    return fundingText, funding


def extract_pub_info(wos_id, elem):
    pub = {"wos_id": wos_id}

    try:
        pub.update(list(elem.iterfind("./static_data/summary/pub_info"))[0].attrib)
        pub.update(list(elem.iterfind("./static_data/summary/pub_info/page"))[0].attrib)
    except Exception as e:
        print("Caught error {0}".format(e))
        logging.error(
            "{0} Could not capture pub_info, Skipping document.".format(wos_id)
        )
        raise

    # Get title, source, and source abbreviations
    for i in elem.iterfind("./static_data/summary/titles/title"):
        pub[str(i.attrib["type"])] = i.text

    pub["title"] = pub["item"]

    # Get document type
    try:
        pub["doc_type"] = list(elem.iterfind("./static_data/summary/doctypes/doctype"))[
            0
        ].text
    except Exception:
        logging.warn(
            "{0} Could not capture doctype, setting to default NULL".format(wos_id)
        )
        pub["doc_type"] = "NULL"

    # Add accession_no and issn
    for item in list(
        elem.iterfind("./dynamic_data/cluster_related/identifiers/identifier")
    ):
        # print item.tag, item.attrib, item.text
        pub[item.attrib["type"]] = item.attrib["value"]

    languages = []
    for lang in list(
        elem.iterfind("./static_data/fullrecord_metadata/languages/language")
    ):
        # print lang.tag, lang.attrib, lang.text
        languages.extend([{"wos_id": wos_id, "language": lang.text}])
    # Get categorical data
    headings = []
    for x in list(
        elem.iterfind(
            "./static_data/fullrecord_metadata/category_info/headings/heading"
        )
    ):
        headings.extend([{"wos_id": wos_id, "heading": x.text}])

    subheadings = []
    for sub in list(
        elem.iterfind(
            "./static_data/fullrecord_metadata/category_info/subheadings/subheading"
        )
    ):
        # print sub.tag, sub.attrib, sub.text
        subheadings.extend([{"wos_id": wos_id, "subheading": sub.text}])
    # print "Subheadings : ", subheadings

    subjects = []
    for sub in list(
        elem.iterfind(
            "./static_data/fullrecord_metadata/category_info/subjects/subject"
        )
    ):
        # print sub.tag, sub.attrib, sub.text
        subjects.extend(
            [
                {
                    "wos_id": wos_id,
                    "ascatype": sub.attrib["ascatype"],
                    "subject": sub.text,
                }
            ]
        )

    for item in list(
        elem.iterfind("./dynamic_data/cluster_related/identifiers/identifier")
    ):
        pub[item.attrib["type"]] = item.attrib["value"]

    # Find the oases type gold status
    for item in list(elem.iterfind("./dynamic_data/ic_related/oases/oas")):
        if item.text == "Yes" and item.attrib["type"] == "gold":
            pub["oases_type_gold"] = "Yes"
            # print "Gold = Yes"

    # Add the abstract
    abstract_text = "NULL"
    for ab in list(
        elem.iterfind(
            "./static_data/fullrecord_metadata/abstracts/abstract/abstract_text/p"
        )
    ):
        if abstract_text == "NULL":
            abstract_text = ""
        abstract_text = abstract_text + "\n<p>" + ab.text + "</p>"
    pub["abstract"] = abstract_text

    return [pub], languages, headings, subheadings, subjects


def extract_keywords(wos_id, elem):
    keywords = []
    keywordsplus = []

    for keyword in list(
        elem.iterfind("./static_data/fullrecord_metadata/keywords/keyword")
    ):
        # print "Keyword :", keyword.text
        keywords.extend([{"wos_id": wos_id, "keyword": keyword.text}])

    for keyword in list(elem.iterfind("./static_data/item/keywords_plus/keyword")):
        # print "Plus ", keyword.tag, keyword.text
        keywordsplus.extend([{"wos_id": wos_id, "keyword": keyword.text}])

    return keywords, keywordsplus


def extract_unindexed_authors(unindexed_pubs):
    """Extracts authors from unindexed publications in the references"""
    authors = list()
    for pub in unindexed_pubs:
        author = dict()
        author["wos_id"] = pub["wos_id"]
        author["role"] = "author"
        author["display_name"] = pub["author"]
        author["full_name"] = pub["author"]
        authors.append(author)
    return authors


def extract_unindexed_publications(wos_id, elem):
    """Extracts info on unindexed publications from the references"""
    unindexed_pubs = []
    for ref in elem.iterfind("./static_data/fullrecord_metadata/references/reference"):
        uid = ref.find("uid").text
        # An unindexed publication's UID does not start with WOS
        if not uid.startswith("WOS"):
            pub = dict()
            pub["wos_id"] = uid

            pub_to_ref = {
                "doi": "doi",
                "author": "citedAuthor",
                "title": "citedTitle",
                "source": "citedWork",
                "pubyear": "year",
                "vol": "volumne",
                "begin": "page",
            }
            for pub_field, ref_field in pub_to_ref.items():
                if (child := ref.find(ref_field)) is not None:
                    pub[pub_field] = child.text

            unindexed_pubs.append(pub)

    return unindexed_pubs


# From stackoverflow
# http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def chunks(iterable, count):
    """Yield successive n-sized chunks from iterable."""
    for i in range(0, len(iterable), count):
        yield iterable[: i + count]


def dump(data, header, sql_header, table_name, file_name, data_format="sql"):
    chunk_size = 1000

    print("Writing out {0} to {1}".format(table_name, file_name))

    if data_format == "sql":
        print("Writing SQL output")
        with open(file_name, "w") as f_handle:
            f_handle.write(sql_header.format(table_name))
            f_handle.write("\n")
            for chunk in chunks(data, chunk_size):
                f_handle.write(
                    "INSERT IGNORE INTO {0} ({1})\n".format(
                        table_name, ", ".join(header)
                    )
                )
                f_handle.write("VALUES\n")

                for idx, row in enumerate(chunk):
                    f_handle.write("(")
                    f_handle.write(
                        ",".join([json.dumps(row.get(attr, "NULL")) for attr in header])
                    )
                    f_handle.write(")")
                    if idx == len(chunk) - 1:
                        f_handle.write(";")
                    else:
                        f_handle.write(",")
                    f_handle.write("\n")

    elif data_format == "json":
        print("Writing json")
        datadict = {table_name: data}
        with open(file_name, "wb") as f_handle:
            json.dump(datadict, f_handle)
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

    args = parser.parse_args()

    print("Processing : {0}".format(args.logfile))

    logging.basicConfig(
        filename=args.logfile,
        level=log_levels[args.verbosity],
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
    )

    logging.debug("Document processing starts")

    context = load_data(args.sourcefile)
    print("Done loading data into etree context")
    total = 0
    bad = 0
    for event, elem in context:
        if event != "start":
            continue
        pub = {}
        if elem.tag == "REC":
            total += 1
            try:
                wos_id = list(elem.iterfind("UID"))[0].text

                pub = extract_pub_info(wos_id, elem)
                publisher = extract_publisher(wos_id, elem)
                authors = extract_authors(wos_id, elem)

            except Exception:
                print("Skipping... {0}".format(wos_id))
                bad += 1
        elem.clear()

    logging.debug(
        "Document Complete:{0} with bad/total lines : {1}/{2}".format(
            args.sourcefile, bad, total
        )
    )
    print("Done")
