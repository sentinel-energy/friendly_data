from datapackage import Package


def test_pathstring(tmp_pkgdir):
    _, dest = tmp_pkgdir
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "licenses": "CC0-1.0",
        "keywords": ["foo", "bar", "baz"],
    }
    pkg = Package(meta, base_path=str(dest))

    fpath = "inputs/names.csv"
    assert dest / fpath
    pkg.infer(fpath)

    res, *_ = pkg.descriptor["resources"]
    print(res)
    assert res["path"].count("\\") > 0
