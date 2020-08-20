import numpy as np


def project_p2a(p, a):
    return a*(np.sum(p*a, axis=1)/np.linalg.norm(a, axis=1)**2)[:, np.newaxis]


def projorth_p2a(p, a):
    return p - project_p2a(p, a)


def phiorth_a(p1, p2, a):
    po1 = projorth_p2a(p1, a)
    po2 = projorth_p2a(p2, a)
    return np.sum(po1*po2, axis=1)/np.linalg.norm(po1, axis=1)/np.linalg.norm(po2, axis=1)


def cos_a(p, a):
    return np.sum(p*a, axis=1)/np.linalg.norm(p, axis=1)/np.linalg.norm(a, axis=1)


def cos_diff(th1, th2, phi):
    return np.cos(th1)*np.cos(th2) + np.sin(th1)*np.sin(th2)*np.cos(phi)
