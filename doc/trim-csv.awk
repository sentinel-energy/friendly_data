#!/usr/bin/env -S awk -f

BEGIN { t1 = 0; t2 = 0; t3 = 0 }
NR == 1;
/wind_offshore/ {
    if (++t1 > 2) next;
    print $0
}
/wind_onshore/ {
    if (++t2 > 2) next;
    if (t2 == 1) {
	print "...,...,...,...,...,..."
    }
    print $0
}
/nuclear/ {
    if (++t3 > 2) next;
    if (t3 == 1) {
	print "...,...,...,...,...,..."
    }
    print $0
}
/Primary Energy|Wind/ {
    if (++t1 > 2) next;
    print $0
}
/Primary Energy|Nuclear/ {
    if (++t2 > 2) next;
    if (t2 == 1) {
	print "...,...,...,...,...,..."
    }
    print $0
}
