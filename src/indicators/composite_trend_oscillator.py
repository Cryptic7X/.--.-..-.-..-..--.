"""
Composite Trend Oscillator - Full Pine Script Implementation
Exact replication of your provided Pine Script
"""

import pandas as pd
import numpy as np
import math

class CompositeTrendOscillator:
    def __init__(self, spacing=3, signal_length=20, filter_type="PhiSmoother", 
                 post_smooth_length=1, upper_trim=0, lower_trim=0, phase=3.7):
        self.spacing = spacing
        self.signal_length = signal_length
        self.filter_type = filter_type
        self.post_smooth_length = post_smooth_length
        self.upper_trim = upper_trim
        self.lower_trim = lower_trim
        self.phase = phase
        
        # Cache for PhiSmoother coefficients
        self.phi_coefficients_cache = {}
    
    def calculate_phi_coefficients(self, length, phase=3.7):
        """Calculate PhiSmoother coefficients exactly as Pine Script"""
        cache_key = (length, phase)
        if cache_key in self.phi_coefficients_cache:
            return self.phi_coefficients_cache[cache_key]
        
        coefs = []
        W = 0.0
        SQRT_PIx2 = math.sqrt(2.0 * math.pi)
        MULTIPLIER = -0.5 / 0.93
        length_2 = length * 0.52353
        
        for i in range(length):
            alpha = (i + phase - length_2) * MULTIPLIER
            beta = 1.0 / (0.2316419 * abs(alpha) + 1.0)
            phi = (math.exp(alpha * alpha * -0.5) * -0.398942280) * beta * (
                0.319381530 + beta * (
                    -0.356563782 + beta * (
                        1.781477937 + beta * (
                            -1.821255978 + beta * 1.330274429
                        )
                    )
                )
            ) + 1.011
            
            if alpha < 0.0:
                phi = 1.0 - phi
            
            weight = phi / SQRT_PIx2
            coefs.append(weight)
            W += weight
        
        result = (coefs, W)
        self.phi_coefficients_cache[cache_key] = result
        return result
    
    def phi_smoother(self, source, length, phase=3.7):
        """Exact replication of Pine Script PhiSmoother"""
        if length <= 1:
            return source.copy()
        
        coefs, W = self.calculate_phi_coefficients(length, phase)
        result = np.zeros_like(source)
        
        for i in range(len(source)):
            if i == 0:
                sma2 = source[i]
            else:
                sma2 = (source[i] + source[i-1]) / 2.0
            
            E = 0.0
            for j in range(min(length, i + 1)):
                if i - j >= 0:
                    if i - j == 0:
                        prev_sma2 = source[i-j]
                    else:
                        prev_sma2 = (source[i-j] + source[i-j-1]) / 2.0 if i-j-1 >= 0 else source[i-j]
                    E += coefs[j] * prev_sma2
            
            result[i] = E / W if W != 0 else source[i]
        
        return result
    
    def ema_filter(self, source, length):
        """EMA calculation matching Pine Script"""
        alpha = 2.0 / (length + 1)
        result = np.zeros_like(source)
        result[0] = source[0]
        
        for i in range(1, len(source)):
            result[i] = alpha * source[i] + (1.0 - alpha) * result[i-1]
        
        return result
    
    def sma_filter(self, source, length):
        """Simple Moving Average"""
        return pd.Series(source).rolling(window=length, min_periods=1).mean().values
    
    def apply_filter(self, source, length, filter_type):
        """Apply the specified filter type"""
        if length <= 1:
            return source
        
        if filter_type == "PhiSmoother":
            return self.phi_smoother(source, length, self.phase)
        elif filter_type == "EMA":
            return self.ema_filter(source, length)
        else:  # Default to SMA
            return self.sma_filter(source, length)
    
    def calculate_scores(self, filter_cluster):
        """Calculate scores for each filter in the cluster"""
        num_filters = len(filter_cluster)
        scores = []
        
        for i in range(num_filters):
            current_filter = filter_cluster[i]
            score = np.zeros_like(current_filter)
            
            for bar in range(len(current_filter)):
                score_sum = 0
                current_value = current_filter[bar]
                
                for j in range(num_filters):
                    if i != j:
                        check_value = filter_cluster[j][bar]
                        polarity = 1 if i < j else -1
                        
                        if current_value > check_value:
                            score_sum += polarity
                        else:
                            score_sum -= polarity
                
                score[bar] = score_sum
            
            scores.append(score)
        
        return scores
    
    def calculate_net_score(self, scores):
        """Calculate net score from individual scores"""
        num_scores = len(scores)
        length = len(scores[0])
        net_score = np.zeros(length)
        
        for bar in range(length):
            score_sum = sum(score[bar] for score in scores)
            avg_score = score_sum / num_scores
            value = num_scores - 1
            net_score[bar] = ((avg_score + value) / (value * 2.0) - 0.5) * 200.0
        
        return net_score
    
    def calculate_signals(self, df, source_col='close'):
        """Calculate CTO signals and return DataFrame with score and signal lines"""
        source = df[source_col].values
        
        if len(source) < 100:
            raise ValueError("Need at least 100 data points for reliable calculation")
        
        # Create filter cluster (33 filters total including raw source)
        filter_cluster = []
        
        # Add raw source (index 0)
        filter_cluster.append(source)
        
        # Add filtered versions (indices 1-32)
        for i in range(1, 33):
            length = i * self.spacing
            filtered = self.apply_filter(source, length, self.filter_type)
            filter_cluster.append(filtered)
        
        # Apply trimming
        if self.upper_trim > 0:
            filter_cluster = filter_cluster[self.upper_trim:]
        
        if self.lower_trim > 0 and len(filter_cluster) > self.lower_trim:
            filter_cluster = filter_cluster[:-self.lower_trim]
        
        # Calculate scores
        scores = self.calculate_scores(filter_cluster)
        
        # Calculate net score
        net_score = self.calculate_net_score(scores)
        
        # Apply post-smoothing
        if self.post_smooth_length > 1:
            net_score = self.phi_smoother(net_score, self.post_smooth_length, 3.7)
        
        # Calculate signal line
        if self.signal_length >= 2:
            sma2_score = self.sma_filter(net_score, 2)
            signal_line = self.phi_smoother(sma2_score, self.signal_length, 3.7)
        else:
            signal_line = np.full_like(net_score, np.nan)
        
        # Create result DataFrame
        result_df = df.copy()
        result_df['cto_score'] = net_score
        result_df['cto_signal'] = signal_line
        
        return result_df
    
    def detect_cto_conditions(self, df, overbought_threshold=70, oversold_threshold=-70):
        """Detect CTO overbought/oversold conditions"""
        score = df['cto_score'].values
        
        # Detect conditions
        overbought = score >= overbought_threshold
        oversold = score <= oversold_threshold
        
        df['cto_overbought'] = overbought
        df['cto_oversold'] = oversold
        
        return df
