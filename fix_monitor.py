import sys

with open('scripts/paper_trade_monitor.py', 'r') as f:
    code = f.read()

fl_old = """                    _is_fast_lane = (_t_score and _t_score >= 100
                                       and _v_score and _v_score >= 100
                                       and _s_score and _s_score >= 100
                                       and _m_score and _m_score >= 100
                                       and _fl_bs_ratio >= 2.0
                                       and _fl_pc_m5 > 5.0  # 5min price must be > 5.0%
                                       and _fl_sig_type == 'ATH')  # NOT_ATH → SmartEntry"""

fl_new = """                    _is_fast_lane = (_t_score and _t_score >= 100
                                       and _v_score and _v_score >= 100
                                       and _s_score and _s_score >= 100
                                       and _m_score and _m_score >= 100
                                       and _fl_bs_ratio >= 2.0
                                       and _fl_pc_m5 > 15.0  # USER UPDATE: 5min price must be > 15.0%
                                       and _fl_sig_type == 'ATH')  # NOT_ATH → SmartEntry"""

if fl_old in code:
    code = code.replace(fl_old, fl_new)
    with open('scripts/paper_trade_monitor.py', 'w') as f:
        f.write(code)
    print("Fixed pc_m5 > 15.0 in Fast Lane")
else:
    print("Could not find Fast Lane block")
