import re
import warnings

import lightgbm as lgb
import numpy as np
import pandas as pd
import statsmodels.api as sm
from joblib import dump
from mlforecast import MLForecast
from mlforecast.lag_transforms import RollingMean
from mlforecast.utils import PredictionIntervals
from scipy.signal import butter, filtfilt, find_peaks, peak_prominences
from utilsforecast.feature_engineering import fourier
import os

warnings.filterwarnings("ignore")


class CustomPreprocessor():
    def __init__(self, freq=None, static_features=[], categorical_feats=[], start_date = None, end_date=None, custom_model = None):
        self.freq = freq
        self.static_features = static_features
        self.categorical_feats = categorical_feats
        self.min_lags_value = None
        self.split = None
        self.frac = None
        self.n_windows = None
        self.data = None
        self.mlf_preprocess = None
        self.start_date = start_date
        self.end_date = end_date
        self.max_lags_value = None
        self.custom_model = custom_model

    def fit(self, df):
        data = df.copy()
        data = df.copy()
        # Calculate initial frequency
        freq_ini = int(data["ds"].diff().median().total_seconds())
        if self.freq is None:
            self.freq = freq_ini
        else:
            if self.freq < freq_ini:
                print(
                    "Initial frequency is longer than the one given, you may"
                    +" need to improve it for good result"
                )
                self.freq = freq_ini

        # Resample data
        if self.freq == 0:
            self.freq = 1
        data.index = data["ds"]
        data = data.resample(f"{int(self.freq)}s").first()
        data = data.bfill().ffill()
        data["ds"] = data.index
        data = data.reset_index(drop=True)

        borne = int(len(data) / 4)

        # Calculate y_lags and season
        data["y_lags"] = data["y"].rolling(window=int(len(data) * 0.02)).mean().diff().bfill()
        season = self.find_prominence(data["y_lags"])
        data = data.drop("y_lags", axis=1)
        season = season[0] if season else 1

        # Apply Fourier transformation
        data, _ = fourier(data, freq=f"{self.freq}s", season_length=season, k=1, h=0)

        # Split data into train and test
        split = len(data)
        frac = min(2000, int(split / 100))
        train = data.copy()

        # Calculate lags and lags_trans
        window_size = int(borne * 2 / 10)
        if window_size < 2:
            window_size = 2
            lags_pos = []
        else:
            lags_pos = [int(x * window_size) for x in range(1, 4)]
        lags_pos = [lag for lag in lags_pos if lag < 3600 * 24 * 30 * 3 / self.freq]

        lags = self.find_lags(train["y"][: int(split / 4)])
        lags = list(set(lags + lags_pos))
        for x in lags[:]:
            lags.extend([x - 1, x, x + 1])

        lags_trans = {
            lag: [RollingMean(min_samples=1, window_size=window_size)]
            for lag in lags_pos
            if lag > 0
        }

        # Store calculated values
        self.split = split
        self.frac = frac
        n_windows = int(6 * frac / 10)
        if n_windows < 2:
            n_windows = 2
        self.n_windows = n_windows
        lags = set(lags)
        lags = [int(lag) for lag in lags if lag > 10]

        self.min_lags_value = (
            min(min(lags), int(window_size / 2)) if lags else int(window_size / 2)
        )
        self.max_lags_value = max(max(lags), max(lags_pos) + window_size)

        if self.custom_model :
            model = {"model_prediction" : self.custom_model }
        else :
            model = {"model_prediction" : lgb.LGBMRegressor(random_state=0, verbosity=-1, n_jobs=-1, categorical_feature=self.categorical_feats,
                                      n_estimators = 200, objective ='regression', num_leaves=100,
                                      )}

        if self.mlf_preprocess is None:
            self.mlf_preprocess = MLForecast(
                models=model,
                freq=str(self.freq)+'s',
                lags = lags,
                lag_transforms=lags_trans,
            )
        self.data = self.mlf_preprocess.preprocess(train)

        def filter_dates(df, start, end):
            return df[(df["ds"] >= start) & (df["ds"] <= end)]

        if self.start_date is not None and self.end_date is not None:
            if len(self.start_date) != len(self.end_date):
                raise ValueError("Start date and End date should have the same length")
            filtered_data = pd.DataFrame()

            for start, end in zip(self.start_date, self.end_date):
                filtered_data = pd.concat([filtered_data, filter_dates(self.data, start, end)])
            self.data = filtered_data.reset_index(drop=True)

        return self

    def predict(self, x, target):
        data = x.copy()
        data = data.rename(columns={target: "y"})
        freq_ini = int(data["ds"].diff().median().total_seconds())
        if self.freq is None:
            self.freq = freq_ini
        else:
            if self.freq < freq_ini:
                print(
                    "Initial frequency is longer than the one given, you may"+
                    " need to improve it for good result"
                )
                self.freq = freq_ini

        # Resample data
        if self.freq == 0:
            self.freq = 1
        data.index = data["ds"]
        data = data.resample(f"{int(self.freq)}s").first()
        data = data.bfill().ffill()
        data["ds"] = data.index
        data = data.reset_index(drop=True)

        features = self.mlf_preprocess.ts.features_order_
        freq = self.freq
        pattern = r"^lag(\d+)$"
        lags = []
        for s in features:
            # Utiliser re.match pour vérifier si la string correspond au pattern
            match = re.match(pattern, s)
            if match:
                # Si la string correspond au pattern, extraire le nombre associé
                lag_value = int(match.group(1))  # Convertir le nombre en entier
                lags.append(lag_value)

        cos1_element = next((s for s in features if s.startswith("cos1_")), None)
        if cos1_element:
            # Getting the integer after 'cos1_'
            entier_str = cos1_element[len("cos1_") :]
            season = int(entier_str)
            data, _ = fourier(data, freq=str(freq) + "s", season_length=season, k=1, h=0)
        last_date = data["ds"].max()
        new_dates = [
            last_date + pd.Timedelta(seconds=freq * i) for i in range(1, self.min_lags_value + 1)
        ]
        new_data = {"ds": new_dates, "y": [0] * self.min_lags_value, "unique_id": "ID"}
        df = pd.DataFrame(new_data)
        data = pd.concat([data, df], ignore_index=True)
        self.data = self.mlf_preprocess.preprocess(data)
        return self

    def lags_fft(self, data, nlags=4):
        lags = []

        for i in range(nlags):
            fft_result = np.fft.fft(data)
            frequencies = np.fft.fftfreq(len(fft_result))

            # Keeping the positive frequencies
            indices_positive = frequencies > 0
            fft_result_positive = fft_result[indices_positive]

            height = max(np.abs(fft_result_positive)) / 10

            # Find the index of fft peaks
            indices_pics, _ = find_peaks(np.abs(fft_result_positive), height=height, distance=5)

            indices = [int(len(data) / x) for x in indices_pics]

            # Limitation of the period found by the FFT
            if indices == [] or min(indices) > len(data) / 10:
                break
            lags.append(min(indices))

            # Low pass filter
            cutoff_frequency = 1 / min(indices)
            order = 4
            try:
                b, a = butter(order, cutoff_frequency, btype="low", analog=False)
                data = filtfilt(b, a, data)
            except np.linalg.LinAlgError as e:
                print(f"Error : {e}")
                return lags, data
        return lags, data

    def find_prominence(self, data, nlags=6, borne=None):
        # Calculating acf
        acf, ci = sm.tsa.acf(data, alpha=0.05, nlags=int(len(data)))
        if borne is None:
            borne = int(len(acf) * 0.05)
        # borne = 0
        distance = int(borne / 5)
        if distance < 1:
            distance = 1
        peaks = [
            element + borne
            for element in list(find_peaks(acf[borne:], prominence=0.01, distance=distance)[0])
        ]
        # Checking if the number of peaks found is more than the limit of lags,
        # if not just return peak
        if len(peaks) < nlags + 1:
            return peaks
        else:
            # Using prominence to sort peaks
            prominence = list(peak_prominences(acf, peaks)[0])
            indices = np.argsort(prominence[4:])[::-1]

            best_prominence = list(indices[:nlags] + 4)
            best = []
            # Find and return the corresponding value
            for i in best_prominence:
                best.append(peaks[i])
        return best

    def find_lags(self, data):
        lags, data_filtered = self.lags_fft(data, nlags=4)
        lags = list(set(self.find_prominence(data, nlags=5) + lags))
        lags = [x for x in lags if x not in [1, 2, 3]]
        return lags



class MLForecastEstimator():
    def __init__(self, freq, categorical_feats, static_features, min_lags_value, split, frac, n_windows, custom_model):
        self.freq = freq
        self.categorical_feats = categorical_feats
        self.static_features = static_features
        self.mlf = None
        self.min_lags_value = min_lags_value
        self.split = split
        self.frac = frac
        self.n_windows = n_windows
        self.h = 0
        self.custom_model = custom_model

    def fit(self, x):
        start_date = pd.Timestamp("2000-01-01")
        trains = x.copy()
        trains["ds"] = [
            start_date + pd.Timedelta(seconds=i * self.freq) for i in range(len(trains))
        ]
        self.h = min(self.min_lags_value, int(self.split / self.frac))

        prediction_interval = PredictionIntervals(n_windows=self.n_windows, h=self.h)
        if self.custom_model :
            model = {"model_prediction" : self.custom_model }
        else :
            model = {"model_prediction" : lgb.LGBMRegressor(random_state=0, verbosity=-1, n_jobs=-1, categorical_feature=self.categorical_feats,
                                      n_estimators = 200, objective ='regression', num_leaves=100,
                                      )}

        self.mlf = MLForecast(
            models = model,
            freq=f'{self.freq}s',
        )
        self.mlf.fit(
            trains,
            prediction_intervals=prediction_interval,
            static_features=self.static_features,
        )

        return self

    def predict(self, x):
        # Prédictions du modèle MLForecast
        predictions = self.mlf.predict(x)
        return predictions


class PipelineMLForecast():

    def __init__(self, df, static_features = [], freq=None, categorical_feats = [], target='y',
                 time_col = 'ds', model_name=None, start_date = None, end_date=None, custom_model=None):
        """Arguments :
        - df : pandas dataframe
        - static_features = list of features in df that should be consider
        as static by the models != exogenous variables
        -freq : time in seconds between every point, shouldn't be lower that the original frequency on df
        - categorical_feats
        -target : name of the target columns that we want to predict
        -time_col : name of the time column
        -model_name : name of the model after saving
        -start_date : list of start date for the training, len(start_date)=len(end_date)
        -end_date : list of end date for training, len(end_date)=len(start_date)
        -custom_model : Function used to train and predict, the default is lgb.LGBMRegressor.
        """
        data = df.copy()
        data.rename(columns={target: "y", time_col: "ds"}, inplace=True)
        data["ds"] = pd.to_datetime(data["ds"])
        data["unique_id"] = "ID"
        self.target = target
        self.time_col = time_col
        self.static_features = static_features
        freq_ini = int(data["ds"].diff().median().total_seconds())
        self.freq = freq
        if self.freq is None:
            self.freq = freq_ini
        self.categorical_feats = categorical_feats
        self.data = data.copy()
        self.preprocessor = None
        self.estimator = None
        if model_name is None:
            self.model_name = "model" + str(self.freq) + "s"
        self.start_date = start_date
        self.end_date = end_date
        self.custom_model = custom_model

    def fit(self) :
        preprocessor = CustomPreprocessor(freq=self.freq, static_features=self.static_features,
                                          categorical_feats=self.categorical_feats,
                                          start_date = self.start_date, end_date = self.end_date, custom_model = self.custom_model)
        preprocessor.fit(self.data)
        self.data = preprocessor.data
        estimator = MLForecastEstimator(freq=self.freq, categorical_feats=self.categorical_feats, static_features=self.static_features, min_lags_value=preprocessor.min_lags_value, split=preprocessor.split, frac=preprocessor.frac,
                                        n_windows=preprocessor.n_windows, custom_model = self.custom_model)

        estimator.fit(self.data)
        self.preprocessor = preprocessor
        self.estimator = estimator
        return self

    # You can give data with every information and target completion with 0, or give the
    # exogenous data to prediction_length is not working yet
    def predict(self, data=None, levels=[99], x_df=None, start_pred=None, prediction_length=None):
        if data is None:
            data = self.data
            preprocessor = self.preprocessor
            h = preprocessor.min_lags_value
        else:
            data.rename(columns={self.target: "y", self.time_col: "ds"}, inplace=True)
            data["ds"] = pd.to_datetime(data["ds"])
            data["unique_id"] = "ID"
            ref_date = data["ds"].iloc[-1]

            if x_df is not None:
                x_df.rename(columns={self.time_col: "ds"}, inplace=True)
                x_df["ds"] = pd.to_datetime(x_df["ds"])
                data = pd.merge(data, x_df, on="ds", how="outer")
                data = data.reset_index(drop=True)

            preprocessor = self.preprocessor.predict(data, self.target)
            data = preprocessor.data
            data = data.ffill().bfill()
            data = data.reset_index(drop=True)

            h = len(data) - data["ds"].sub(ref_date).abs().idxmin() - 1
            if h < 0:
                h = 0

        if start_pred is not None:
            if isinstance(start_pred, str):
                start_pred = pd.to_datetime(start_pred)
            new_h = len(data) - data["ds"].sub(start_pred).abs().idxmin() - 1
            if new_h <= 0 or new_h < h:
                raise ValueError(f"not enought data to start from {str(start_pred)}")
            else:
                h = new_h

        length_pred = min(preprocessor.min_lags_value, self.estimator.h)

        if h == 0:
            print("No x_df")
            new_df = data.copy()
            # print(new_df.tail())
        else:
            new_df = data.iloc[:-h].copy()

        if x_df is None :
            preprocessor = preprocessor.predict(data, self.target)
            x_df = preprocessor.data
        else :
            x_df = data

        forecasts = self.estimator.mlf.predict(length_pred, level=levels, X_df=x_df, new_df=new_df)

        if prediction_length is None:
            new_h = 0
        else:
            new_h = prediction_length - length_pred

        #the while is here if we need to predict more than the original prediction length. In this case,
        #the predictions will because less precise in the future because we will base our predictions on our
        #previous predictions

        while new_h>=0 :
            merged_data = pd.merge(data, forecasts, on='ds', how='outer')
            merged_data.loc[~merged_data['model_prediction'].isna(), 'y'] = merged_data['model_prediction']
            merged_data.drop('model_prediction', axis=1, inplace=True)
            cols_to_drop = [col for col in merged_data.columns if ('lag' in col) or ('model_prediction' in col)]+['unique_id_y']
            merged_data.drop(cols_to_drop, axis=1, inplace=True)
            merged_data = merged_data.rename(columns={"unique_id_x": "unique_id"})
            merged_data = preprocessor.mlf_preprocess.preprocess(merged_data)
            h = h - length_pred
            new_df = merged_data.iloc[:-h].copy()
            x_df = merged_data
            length = min(length_pred, new_h)
            fore = self.estimator.mlf.predict(length, level=levels, X_df=x_df, new_df=new_df)
            forecasts = pd.concat([forecasts, fore])
            new_h = new_h - length_pred

        return forecasts.reset_index(drop=True).iloc[:prediction_length]

    def save(self, file_path="./", name=None):
        if name is None:
            name = self.model_name
        path = os.path.join(file_path, name + ".joblib")
        dump(self, path)

    def get_length_prediction(self):
        return self.preprocessor.min_lags_value * self.freq

    def get_length_needed(self):
        return self.preprocessor.max_lags_value * self.freq

    def get_features_used(self):
        return self.preprocessor.mlf_preprocess.ts.features_order_
