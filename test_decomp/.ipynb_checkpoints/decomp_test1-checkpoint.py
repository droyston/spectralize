
import librosa

audio_data = '/home/dylanroyston/Music/spectralize_data/Bastille - Bad Blood.mp3'

x , sr = librosa.load(audio_data)
print(type(x), type(sr))
print(x.shape, sr)


